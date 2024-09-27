# SPDX-FileCopyrightText: Copyright (c) 2023 - 2024 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import torch
import hydra
from hydra.utils import to_absolute_path
import wandb
import matplotlib.pyplot as plt

from torch.nn.parallel import DistributedDataParallel
from omegaconf import DictConfig

from modulus.models.afno import AFNO
from modulus.datapipes.climate import ERA5HDF5Datapipe
from modulus.distributed import DistributedManager
from modulus.utils import StaticCaptureTraining, StaticCaptureEvaluateNoGrad

from modulus.launch.logging import LaunchLogger, PythonLogger, initialize_mlflow
from modulus.launch.utils import load_checkpoint, save_checkpoint

#cdj
from modulus.utils.weighted_acc_rmse import weighted_rmse_torch_channels, weighted_acc_torch_channels
import numpy as np
import h5py

try:
    from apex import optimizers
except:
    raise ImportError(
        "FCN training requires apex package for optimizer."
        + "See https://github.com/nvidia/apex for install details."
    )


def loss_func(x, y, p=2.0):
    yv = y.reshape(x.size()[0], -1)
    xv = x.reshape(x.size()[0], -1)
    diff_norms = torch.linalg.norm(xv - yv, ord=p, dim=1)
    y_norms = torch.linalg.norm(yv, ord=p, dim=1)

    return torch.mean(diff_norms / y_norms)


@torch.no_grad()
def autoregressive_inference(eval_step, fcn_model, datapipe, channels, epoch, ckpt_path):
    #cdj
    device = torch.cuda.current_device() if torch.cuda.is_available() else 'cpu'
    file_dir = os.path.join(to_absolute_path(ckpt_path),'inf_2')
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    
    loss_epoch = 0
    num_examples = 0  # Number of validation examples
    # Dealing with DDP wrapper
    if hasattr(fcn_model, "module"):
        fcn_model = fcn_model.module
    fcn_model.eval()
    
    valid_loss_all = []
    acc_all = []       
    for ics, data in enumerate(datapipe):
        print("Inference for {} initial conditions".format(ics))
        invar = data[0]["invar"].detach() # torch.Size([1, 5, 164, 168])
        outvar = data[0]["outvar"].detach() # torch.Size([1, 8, 5, 164, 168]) num_steps_validation: 8
        predvar = torch.zeros_like(outvar)
        
        #cdj
        land_msk = torch.isnan(invar[0,3]).int() # nan_msk.sum().item()
        invar = invar*(1-land_msk.unsqueeze(0).unsqueeze(0))
        outvar = outvar*(1-land_msk.unsqueeze(0).unsqueeze(0).unsqueeze(0))
        
        # if torch.isnan(invar).sum() + torch.isnan(outvar).sum() > 0:
            # print('nan in invar or outvar')
        invar = torch.nan_to_num(invar, nan=0.0)
        outvar = torch.nan_to_num(outvar, nan=0.0)      

        #initialize memory for image sequences and RMSE/ACC, tqe for precip
        valid_loss = torch.zeros((outvar.shape[1], len(channels))).to(device, dtype=torch.float)
        acc = torch.zeros((outvar.shape[1], len(channels))).to(device, dtype=torch.float)
        acc_unweighted = torch.zeros((outvar.shape[1], len(channels))).to(device, dtype=torch.float)
        tqe = torch.zeros((outvar.shape[1], len(channels))).to(device, dtype=torch.float)        
        for t in range(outvar.shape[1]):
            output = eval_step(fcn_model, invar)
            invar.copy_(output)
            predvar[:, t] = output.detach().cpu()*(1-land_msk.unsqueeze(0).unsqueeze(0).cpu()) #cdj predvar torch.Size([1, 8, 5, 164, 168])
            #cdj
            for ich in range(len(channels)):
                pred = torch.unsqueeze(predvar[:, t, ich], 0)
                tar = torch.unsqueeze(outvar[:, t, ich], 0)            
                valid_loss[t,ich] = weighted_rmse_torch_channels(pred, tar)# predvar torch.Size([1, 41, 5, 164, 168])
                acc[t,ich] = weighted_acc_torch_channels(pred, tar)
                # print('Timestep {} of {}. channel {} RMS Error: {}, ACC: {}'.format((t), outvar.shape[1], ich, valid_loss[t,ich], acc[t,ich]))
            print('Timestep {} of {}. channel {} RMS Error: {}, ACC: {}'.format((t), outvar.shape[1], 3, valid_loss[t,3], acc[t,3]))

        num_elements = torch.prod(torch.Tensor(list(predvar.shape[1:])))
        loss_epoch += torch.sum(torch.pow(predvar - outvar, 2)) / num_elements
        num_examples += predvar.shape[0]

        # Plotting
        if ics == -1: 
            predvar = predvar.cpu().numpy()
            outvar = outvar.cpu().numpy()
            for chan in channels:
                plt.close("all")
                fig, ax = plt.subplots(
                    3, predvar.shape[1], figsize=(15, predvar.shape[0] * 5)
                )
                for t in range(outvar.shape[1]):
                    ax[0, t].imshow(predvar[0, t, chan])
                    ax[1, t].imshow(outvar[0, t, chan])
                    ax[2, t].imshow(predvar[0, t, chan] - outvar[0, t, chan])
                
                fig_name = f"epoch{epoch}_validation_channel{chan}.png"
                fig_save_path = os.path.join(file_dir, fig_name)
                # os.makedirs(os.path.dirname(fig_save_path), exist_ok=True)
                # print("Saving figs at {}".format(fig_save_path))
                fig.savefig(fig_save_path)
        
        vl = np.expand_dims(valid_loss.cpu().numpy(),0)
        ac = np.expand_dims(acc.cpu().numpy(),0)
        if ics ==0 or len(valid_loss_all) == 0:
            valid_loss_all = vl
            acc_all = ac
        else:
            valid_loss_all = np.concatenate((valid_loss_all, vl), 0)
            acc_all = np.concatenate((acc_all, ac), 0)
        
    #save predictions and loss
    file_name = 'autoregressive_predictions.h5'
    file_path = os.path.join(file_dir, file_name)
    print("Saving files at {}".format(file_path))
    with h5py.File(file_path, 'a') as f:
    
        try:
            f.create_dataset("rmse", data = valid_loss_all, shape = valid_loss_all.shape, dtype =np.float32)
        except:
            del f["rmse"]
            f.create_dataset("rmse", data = valid_loss_all, shape = valid_loss_all.shape, dtype =np.float32)
            f["rmse"][...] = valid_loss_all

        try:
            f.create_dataset("acc", data = acc_all, shape = acc_all.shape, dtype =np.float32)
        except:
            del f["acc"]
            f.create_dataset("acc", data = acc_all, shape = acc_all.shape, dtype =np.float32)
            f["acc"][...] = acc_all   
        
        f.close()
    
    # fcn_model.train()
    return loss_epoch / num_examples


# @hydra.main(version_base="1.2", config_path="conf", config_name="config_uvsp_swh_mwp")
def main() -> None:
    DistributedManager.initialize()
    dist = DistributedManager()

    LaunchLogger.initialize()  # Modulus launch logger
    logger = PythonLogger("main")  # General python logger
    
    #cdj 
    ckpt_path = "./checkpoints/uvsp_swh_mwp_3/"
    out_of_sample_dir = "/datasets/hdf5_data_uvsp_swh_mwp_1/out_of_sample"
    stats_dir = "/datasets/hdf5_data_uvsp_swh_mwp_1/stats"
    channels = [0, 1, 2, 3, 4]
    num_steps_validation = 20
    num_samples_per_year_train = 1460
    num_samples_per_year_validation = 1
    batch_size_validation = 1
    num_workers_validation = 4

    if dist.rank == 0:
        validation_datapipe = ERA5HDF5Datapipe(
            data_dir=to_absolute_path(out_of_sample_dir),#cdj
            stats_dir=to_absolute_path(stats_dir),
            channels=channels,
            num_steps=num_steps_validation, #prediction_length
            num_samples_per_year=num_samples_per_year_train, #cdj
            num_samples_per_year_validation=num_samples_per_year_validation, #n_ics
            batch_size=batch_size_validation,
            patch_size=(2, 2),#cdj
            device=dist.device,
            num_workers=num_workers_validation,
            shuffle=True,
        )

    fcn_model = AFNO(
        inp_shape=[164, 168], #cdj [165,169]
        in_channels=len(channels),
        out_channels=len(channels),
        patch_size=[2, 2], #cdj
        embed_dim=768,
        depth=12,
        num_blocks=8,
    ).to(dist.device)


    # Initialize optimizer and scheduler
    optimizer = optimizers.FusedAdam(
        fcn_model.parameters(), betas=(0.9, 0.999), lr=1e-6, weight_decay=0.0
    )
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=150)

    # Attempt to load latest checkpoint if one exists
    loaded_epoch = load_checkpoint(
        to_absolute_path(ckpt_path),
        models=fcn_model,
        optimizer=optimizer,
        scheduler=scheduler,
        device=dist.device,
    )

    @StaticCaptureEvaluateNoGrad(model=fcn_model, use_graphs=False)
    def eval_step_forward(my_model, invar):
        return my_model(invar)


    # Main 
    if dist.rank == 0:
        error = autoregressive_inference(
            eval_step_forward, fcn_model, validation_datapipe, channels=channels, epoch=0, ckpt_path=ckpt_path
        )


    scheduler.step()



    if dist.rank == 0:
        logger.info("Finished inference!")


if __name__ == "__main__":
    main()

# SPDX-FileCopyrightText: Copyright (c) 2023 - 2024 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import torch
import hydra
from hydra.utils import to_absolute_path
import wandb
import matplotlib.pyplot as plt

from torch.nn.parallel import DistributedDataParallel
from omegaconf import DictConfig

from modulus.models.afno import AFNO
from modulus.datapipes.climate import ERA5HDF5Datapipe
from modulus.distributed import DistributedManager
from modulus.utils import StaticCaptureTraining, StaticCaptureEvaluateNoGrad

from modulus.launch.logging import LaunchLogger, PythonLogger, initialize_mlflow
from modulus.launch.utils import load_checkpoint, save_checkpoint

#cdj
from modulus.utils.weighted_acc_rmse import weighted_rmse_torch_channels, weighted_acc_torch_channels
import numpy as np
import h5py

try:
    from apex import optimizers
except:
    raise ImportError(
        "FCN training requires apex package for optimizer."
        + "See https://github.com/nvidia/apex for install details."
    )


def loss_func(x, y, p=2.0):
    yv = y.reshape(x.size()[0], -1)
    xv = x.reshape(x.size()[0], -1)
    diff_norms = torch.linalg.norm(xv - yv, ord=p, dim=1)
    y_norms = torch.linalg.norm(yv, ord=p, dim=1)

    return torch.mean(diff_norms / y_norms)


@torch.no_grad()
def autoregressive_inference(eval_step, fcn_model, datapipe, channels, epoch, ckpt_path):
    #cdj
    device = torch.cuda.current_device() if torch.cuda.is_available() else 'cpu'
    file_dir = os.path.join(to_absolute_path(ckpt_path),'inf_2')
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    
    loss_epoch = 0
    num_examples = 0  # Number of validation examples
    # Dealing with DDP wrapper
    if hasattr(fcn_model, "module"):
        fcn_model = fcn_model.module
    fcn_model.eval()
    
    valid_loss_all = []
    acc_all = []       
    for ics, data in enumerate(datapipe):
        print("Inference for {} initial conditions".format(ics))
        invar = data[0]["invar"].detach() # torch.Size([1, 5, 164, 168])
        outvar = data[0]["outvar"].detach() # torch.Size([1, 8, 5, 164, 168]) num_steps_validation: 8
        predvar = torch.zeros_like(outvar)
        
        #cdj
        land_msk = torch.isnan(invar[0,3]).int() # nan_msk.sum().item()
        invar = invar*(1-land_msk.unsqueeze(0).unsqueeze(0))
        outvar = outvar*(1-land_msk.unsqueeze(0).unsqueeze(0).unsqueeze(0))
        
        # if torch.isnan(invar).sum() + torch.isnan(outvar).sum() > 0:
            # print('nan in invar or outvar')
        invar = torch.nan_to_num(invar, nan=0.0)
        outvar = torch.nan_to_num(outvar, nan=0.0)      

        #initialize memory for image sequences and RMSE/ACC, tqe for precip
        valid_loss = torch.zeros((outvar.shape[1], len(channels))).to(device, dtype=torch.float)
        acc = torch.zeros((outvar.shape[1], len(channels))).to(device, dtype=torch.float)
        acc_unweighted = torch.zeros((outvar.shape[1], len(channels))).to(device, dtype=torch.float)
        tqe = torch.zeros((outvar.shape[1], len(channels))).to(device, dtype=torch.float)        
        for t in range(outvar.shape[1]):
            output = eval_step(fcn_model, invar)
            invar.copy_(output)
            predvar[:, t] = output.detach().cpu()*(1-land_msk.unsqueeze(0).unsqueeze(0).cpu()) #cdj predvar torch.Size([1, 8, 5, 164, 168])
            #cdj
            for ich in range(len(channels)):
                pred = torch.unsqueeze(predvar[:, t, ich], 0)
                tar = torch.unsqueeze(outvar[:, t, ich], 0)            
                valid_loss[t,ich] = weighted_rmse_torch_channels(pred, tar)# predvar torch.Size([1, 41, 5, 164, 168])
                acc[t,ich] = weighted_acc_torch_channels(pred, tar)
                # print('Timestep {} of {}. channel {} RMS Error: {}, ACC: {}'.format((t), outvar.shape[1], ich, valid_loss[t,ich], acc[t,ich]))
            print('Timestep {} of {}. channel {} RMS Error: {}, ACC: {}'.format((t), outvar.shape[1], 3, valid_loss[t,3], acc[t,3]))

        num_elements = torch.prod(torch.Tensor(list(predvar.shape[1:])))
        loss_epoch += torch.sum(torch.pow(predvar - outvar, 2)) / num_elements
        num_examples += predvar.shape[0]

        # Plotting
        if ics == -1: 
            predvar = predvar.cpu().numpy()
            outvar = outvar.cpu().numpy()
            for chan in channels:
                plt.close("all")
                fig, ax = plt.subplots(
                    3, predvar.shape[1], figsize=(15, predvar.shape[0] * 5)
                )
                for t in range(outvar.shape[1]):
                    ax[0, t].imshow(predvar[0, t, chan])
                    ax[1, t].imshow(outvar[0, t, chan])
                    ax[2, t].imshow(predvar[0, t, chan] - outvar[0, t, chan])
                
                fig_name = f"epoch{epoch}_validation_channel{chan}.png"
                fig_save_path = os.path.join(file_dir, fig_name)
                # os.makedirs(os.path.dirname(fig_save_path), exist_ok=True)
                # print("Saving figs at {}".format(fig_save_path))
                fig.savefig(fig_save_path)
        
        vl = np.expand_dims(valid_loss.cpu().numpy(),0)
        ac = np.expand_dims(acc.cpu().numpy(),0)
        if ics ==0 or len(valid_loss_all) == 0:
            valid_loss_all = vl
            acc_all = ac
        else:
            valid_loss_all = np.concatenate((valid_loss_all, vl), 0)
            acc_all = np.concatenate((acc_all, ac), 0)
        
    #save predictions and loss
    file_name = 'autoregressive_predictions.h5'
    file_path = os.path.join(file_dir, file_name)
    print("Saving files at {}".format(file_path))
    with h5py.File(file_path, 'a') as f:
    
        try:
            f.create_dataset("rmse", data = valid_loss_all, shape = valid_loss_all.shape, dtype =np.float32)
        except:
            del f["rmse"]
            f.create_dataset("rmse", data = valid_loss_all, shape = valid_loss_all.shape, dtype =np.float32)
            f["rmse"][...] = valid_loss_all

        try:
            f.create_dataset("acc", data = acc_all, shape = acc_all.shape, dtype =np.float32)
        except:
            del f["acc"]
            f.create_dataset("acc", data = acc_all, shape = acc_all.shape, dtype =np.float32)
            f["acc"][...] = acc_all   
        
        f.close()
    
    # fcn_model.train()
    return loss_epoch / num_examples


# @hydra.main(version_base="1.2", config_path="conf", config_name="config_uvsp_swh_mwp")
def main() -> None:
    DistributedManager.initialize()
    dist = DistributedManager()

    LaunchLogger.initialize()  # Modulus launch logger
    logger = PythonLogger("main")  # General python logger
    
    #cdj 
    ckpt_path = "./checkpoints/uvsp_swh_mwp_3/"
    out_of_sample_dir = "/datasets/hdf5_data_uvsp_swh_mwp_1/out_of_sample"
    stats_dir = "/datasets/hdf5_data_uvsp_swh_mwp_1/stats"
    channels = [0, 1, 2, 3, 4]
    num_steps_validation = 20
    num_samples_per_year_train = 1460
    num_samples_per_year_validation = 1
    batch_size_validation = 1
    num_workers_validation = 4

    if dist.rank == 0:
        validation_datapipe = ERA5HDF5Datapipe(
            data_dir=to_absolute_path(out_of_sample_dir),#cdj
            stats_dir=to_absolute_path(stats_dir),
            channels=channels,
            num_steps=num_steps_validation, #prediction_length
            num_samples_per_year=num_samples_per_year_train, #cdj
            num_samples_per_year_validation=num_samples_per_year_validation, #n_ics
            batch_size=batch_size_validation,
            patch_size=(2, 2),#cdj
            device=dist.device,
            num_workers=num_workers_validation,
            shuffle=True,
        )

    fcn_model = AFNO(
        inp_shape=[164, 168], #cdj [165,169]
        in_channels=len(channels),
        out_channels=len(channels),
        patch_size=[2, 2], #cdj
        embed_dim=768,
        depth=12,
        num_blocks=8,
    ).to(dist.device)


    # Initialize optimizer and scheduler
    optimizer = optimizers.FusedAdam(
        fcn_model.parameters(), betas=(0.9, 0.999), lr=1e-6, weight_decay=0.0
    )
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=150)

    # Attempt to load latest checkpoint if one exists
    loaded_epoch = load_checkpoint(
        to_absolute_path(ckpt_path),
        models=fcn_model,
        optimizer=optimizer,
        scheduler=scheduler,
        device=dist.device,
    )

    @StaticCaptureEvaluateNoGrad(model=fcn_model, use_graphs=False)
    def eval_step_forward(my_model, invar):
        return my_model(invar)


    # Main 
    if dist.rank == 0:
        error = autoregressive_inference(
            eval_step_forward, fcn_model, validation_datapipe, channels=channels, epoch=0, ckpt_path=ckpt_path
        )


    scheduler.step()



    if dist.rank == 0:
        logger.info("Finished inference!")


if __name__ == "__main__":
    main()
