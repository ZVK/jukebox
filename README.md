# Jukebox (Dadabots fork)
Code for "Jukebox: A Generative Model for Music"

[Paper](https://cdn.openai.com/papers/jukebox.pdf) 

[Blog](https://openai.com/blog/jukebox) 

[Explorer](http://jukebox.openai.com/) 


## Dadabots contributions
- `utils.queue` new module for mysql database interactions (job queue)
- `utils.lyric_align` lyric alignment scores (lyric correctness metrics)
- `sample.py` improved sample interface using jobs database and new modules

# Install
``` 
# Required: Sampling
conda create --name jukebox python=3.7.5
conda activate jukebox
conda install mpi4py=3.0.3
conda install pytorch=1.4 torchvision=0.5 cudatoolkit=10.0 -c pytorch
git clone https://github.com/openai/jukebox.git
cd jukebox
pip install -r requirements.txt
pip install -e .

# Required: Training
conda install av=7.0.01 -c conda-forge 
pip install ./tensorboardX
 
# Optional: Apex for faster training with fused_adam
conda install pytorch=1.1 torchvision=0.3 cudatoolkit=10.0 -c pytorch
pip install -v --no-cache-dir --global-option="--cpp_ext" --global-option="--cuda_ext" ./apex
```
# MySQL Setup
Download and create a [MySQL Database](https://www.mysql.com)


Create a new json config file to give Jukebox access to the DB
```
vim ~/jbq_credentials.json
```
Then add the following line with your own credentials and save the file
```
{"host": "mydb.myhost.com", "user": "db_user", "password": "foobar123", "db": "db_name"}
```

The python module `utils.queue` expects a table named `jobs_jukebox` with the following structure:


- `job_id` INT
- `name` TEXT
- `locked` TINYINT
- `status` TEXT
- `params` TEXT
- `log` TEXT
- `date_created` DATETIME
- `date_modified` DATETIME
- `date_done` DATETIME

This [SQL code](https://github.com/ZVK/jukebox/blob/master/jukebox/data/create_table_jobs_jukebox.sql) can be used to create the table:


# Sampling
To sample normally, run the following command. Model can be `5b`, `5b_lyrics`, `1b_lyrics`
``` 
python jukebox/sample.py
```
The above runs the queue from mySQL reading from the `jukebox_jobs` table. 
Any standard sample.py kwargs can be passed through the `params` field in the SQL table.

Here is an example of creating the `params` column:
```
params = {"artist": "the beach boys", "genre": "pop", "model": "", "lyrics": "", "name": "", "length": 60}
```

new jobs can be programmatically created by using the `queue` module

```
db, cur = queue.connect_db()
queue.new_job(cur, name = "newjob1", params = params, status = "top_ready")
```
TODO: make a notebook or web dashboard for the above process of creating new jobs

The samples decoded from each level are stored in `{name}/level_{level}`.
 You can also view the samples as an html with the aligned lyrics under `{name}/level_{level}/index.html`. Run `python -m http.server` and open the html through the server to see the lyrics animate as the song plays.  

The hps are for a V100 GPU with 16 GB GPU memory. The `1b_lyrics`, `5b`, and `5b_lyrics` top-level priors take up 3.8 GB, 10.3 GB, and 11.5 GB, respectively. The peak memory usage to store transformer key, value cache is about 400 MB for `1b_lyrics` and 1 GB for `5b_lyrics` per sample. If you are having trouble with CUDA OOM issues, try `1b_lyrics` or decrease `max_batch_size` in sample.py, and `--n_samples` in the script call.

On a V100, it takes about 3 hrs to fully sample 20 seconds of music. Since this is a long time, it is recommended to use `n_samples > 1` so you can generate as many samples as possible in parallel. The 1B lyrics and upsamplers can process 16 samples at a time, while 5B can fit only up to 3. Since the vast majority of time is spent on upsampling, we recommend using a multiple of 3 less than 16 like `--n_samples 15` for `5b_lyrics`. This will make the top-level generate samples in groups of three while upsampling is done in one pass.

TODO: prompting does not yet work with this fork.

# Training
## VQVAE
To train a small vqvae, run
```
mpiexec -n {ngpus} python jukebox/train.py --hps=small_vqvae --name=small_vqvae --sample_length=262144 --bs=4 --nworkers=4 --audio_files_dir={audio_files_dir} --labels=False --train --aug_shift --aug_blend
```
Here, `{audio_files_dir}` is the directory in which you can put the audio files for your dataset, and `{ngpus}` is number of GPU's you want to use to train. 
The above trains a two-level VQ-VAE with `downs_t = (5,3)`, and `strides_t = (2, 2)` meaning we downsample the audio by `2**5 = 32` to get the first level of codes, and `2**8 = 256` to get the second level codes.  
Checkpoints are stored in the `logs` folder. You can monitor the training by running Tensorboard
```
tensorboard --logdir logs
```
    
## Prior
### Train prior or upsamplers
Once the VQ-VAE is trained, we can restore it from its saved checkpoint and train priors on the learnt codes. 
To train the top-level prior, we can run

```
mpiexec -n {ngpus} python jukebox/train.py --hps=small_vqvae,small_prior,all_fp16,cpu_ema --name=small_prior --sample_length=2097152 --bs=4 --nworkers=4 --audio_files_dir={audio_files_dir} --labels=False --train --test --aug_shift --aug_blend --restore_vqvae=logs/small_vqvae/checkpoint_latest.pth.tar --prior --levels=2 --level=1 --weight_decay=0.01 --save_iters=1000
```

To train the upsampler, we can run
```
mpiexec -n {ngpus} python jukebox/train.py --hps=small_vqvae,small_upsampler,all_fp16,cpu_ema --name=small_upsampler --sample_length 262144 --bs 4 --nworkers 4 --audio_files_dir {audio_files_dir} --labels False --train --test --aug_shift --aug_blend --restore_vqvae logs/small_vqvae/checkpoint_latest.pth.tar --prior --levels 2 --level 0 --weight_decay 0.01 --save_iters 1000
```
We pass `sample_length = n_ctx * downsample_of_level` so that after downsampling the tokens match the n_ctx of the prior hps. 
Here, `n_ctx = 8192` and `downsamples = (32, 256)`, giving `sample_lengths = (8192 * 32, 8192 * 256) = (65536, 2097152)` respectively for the bottom and top level. 

### Reuse pre-trained VQ-VAE and retrain top level prior on new dataset.
Our pre-trained VQ-VAE can produce compressed codes for a wide variety of genres of music, and the pre-trained upsamplers can upsample them back to audio that sound very similar to the original audio.
To re-use these for a new dataset of your choice, you can retrain just the top-level  

To retrain top-level on a new dataset, run
```
mpiexec -n {ngpus} python jukebox/train.py --hps=vqvae,small_prior,all_fp16,cpu_ema --name=pretrained_vqvae_small_prior --sample_length=1048576 --bs=4 --nworkers=4 --bs_sample=4 --aug_shift --aug_blend --audio_files_dir={audio_files_dir} --labels=False --train --test --prior --levels=3 --level=2 --weight_decay=0.01 --save_iters=1000
```

You can then run sample.py with the top-level of our models replaced by your new model. To do so, add an entry `my_model` in MODELs (in `make_models.py`) with the (vqvae hps, upsampler hps, top-level prior hps) of your new model, and run sample.py with `--model=my_model`. 
	

# Citation

Please cite using the following bibtex entry:

```
@article{dhariwal2020jukebox,
  title={Jukebox: A Generative Model for Music},
  author={Dhariwal, Prafulla and Jun, Heewoo and Payne, Christine and Kim, Jong Wook and Radford, Alec and Sutskever, Ilya},
  journal={arXiv preprint arXiv:[TODO]},
  year={2020}
}
```
