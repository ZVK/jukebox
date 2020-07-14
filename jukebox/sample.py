import os
import subprocess
import torch as t

from jukebox.hparams import Hyperparams
from jukebox.utils.torch_utils import empty_cache
from jukebox.utils.audio_utils import save_wav, load_audio
from jukebox.make_models import make_model
from jukebox.align import get_alignment
from jukebox.save_html import save_html
from jukebox.utils.sample_utils import split_batch, get_starts
from jukebox.utils.dist_utils import print_once
import fire


def sample_partial_window(zs, labels, sampling_kwargs, level, prior, tokens_to_sample, hps):
    """Sample a partial window of length<n_ctx with tokens_to_sample new tokens on level=level"""
    z = zs[level]
    n_ctx = prior.n_ctx
    current_tokens = z.shape[1]
    if current_tokens < n_ctx - tokens_to_sample:
        sampling_kwargs['sample_tokens'] = current_tokens + tokens_to_sample
        start = 0
    else:
        sampling_kwargs['sample_tokens'] = n_ctx
        start = current_tokens - n_ctx + tokens_to_sample

    return sample_single_window(zs, labels, sampling_kwargs, level, prior, start, hps)


def sample_single_window(zs, labels, sampling_kwargs, level, prior, start, hps):
    """Sample a single window of length=n_ctx at position=start on level=level"""
    n_samples = hps.n_samples
    n_ctx = prior.n_ctx
    end = start + n_ctx

    # get z already sampled at current level
    z = zs[level][:,start:end]

    if 'sample_tokens' in sampling_kwargs:
        # Support sampling a window shorter than n_ctx
        sample_tokens = sampling_kwargs['sample_tokens']
    else:
        sample_tokens = (end - start)
    conditioning_tokens, new_tokens = z.shape[1], sample_tokens - z.shape[1]

    print_once(f"Sampling {sample_tokens} tokens for [{start},{start+sample_tokens}]. Conditioning on {conditioning_tokens} tokens")

    if new_tokens <= 0:
        # Nothing new to sample
        return zs
    
    # get z_conds from level above
    z_conds = prior.get_z_conds(zs, start, end)

    # set y offset, sample_length and lyrics tokens
    y = prior.get_y(labels, start)

    empty_cache()

    max_batch_size = sampling_kwargs['max_batch_size']
    del sampling_kwargs['max_batch_size']


    z_list = split_batch(z, n_samples, max_batch_size)
    z_conds_list = split_batch(z_conds, n_samples, max_batch_size)
    y_list = split_batch(y, n_samples, max_batch_size)
    z_samples = []
    for z_i, z_conds_i, y_i in zip(z_list, z_conds_list, y_list):
        z_samples_i = prior.sample(n_samples=z_i.shape[0], z=z_i, z_conds=z_conds_i, y=y_i, **sampling_kwargs)
        z_samples.append(z_samples_i)
    z = t.cat(z_samples, dim=0)

    sampling_kwargs['max_batch_size'] = max_batch_size

    # Update z with new sample
    z_new = z[:,-new_tokens:]
    zs[level] = t.cat([zs[level], z_new], dim=1)
    return zs


def sample_level(zs, labels, sampling_kwargs, level, prior, total_length, hop_length, hps):
    """Sample total_length tokens at level=level with hop_length=hop_length"""
    print_once(f"Sampling level {level}")
    if total_length >= prior.n_ctx:
        for start in get_starts(total_length, prior.n_ctx, hop_length):
            zs = sample_single_window(zs, labels, sampling_kwargs, level, prior, start, hps)
    else:
        zs = sample_partial_window(zs, labels, sampling_kwargs, level, prior, total_length, hps)
    return zs


def _sample(zs, labels, sampling_kwargs, priors, sample_levels, hps):
    """Sample multiple levels"""
    alignments = None
    for level in reversed(sample_levels):
        prior = priors[level]
        prior.cuda()
        empty_cache()

        # Set correct total_length, hop_length, labels and sampling_kwargs for level
        assert hps.sample_length % prior.raw_to_tokens == 0, f"Expected sample_length {hps.sample_length} to be multiple of {prior.raw_to_tokens}"
        total_length = hps.sample_length//prior.raw_to_tokens
        hop_length = int(hps.hop_fraction[level]*prior.n_ctx)
        zs = sample_level(zs, labels[level], sampling_kwargs[level], level, prior, total_length, hop_length, hps)

        prior.cpu()
        empty_cache()

        # Decode sample
        x = priors[-1].decode(zs[level:], start_level=level, bs_chunks=zs[level].shape[0])
        logdir = f"{hps.job_id}_{hps.name}/level_{level}"
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        t.save(dict(zs=zs, labels=labels, sampling_kwargs=sampling_kwargs, x=x), f"{logdir}/data.pth.tar")
        save_wav(logdir, x, hps.sr)
        if alignments is None and priors[-1].n_tokens > 0:
            alignments = get_alignment(x, zs, labels[-1], priors[-1], sampling_kwargs[-1]['fp16'], hps)
        save_html(logdir, x, zs, labels[-1], alignments, hps)
    return zs


def ancestral_sample(labels, sampling_kwargs, priors, hps):
    """Generate ancestral samples given a list of artists and genres"""
    sample_levels = list(range(len(priors)))
    zs = [t.zeros(hps.n_samples,0,dtype=t.long, device='cuda') for _ in range(len(priors))]
    zs = _sample(zs, labels, sampling_kwargs, priors, sample_levels, hps)
    return zs


def upsample(zs, labels, sampling_kwargs, priors, hps):
    """Upsample given already generated upper-level codes"""
    sample_levels = list(range(len(priors) - 1))
    zs = _sample(zs, labels, sampling_kwargs, priors, sample_levels, hps)
    return zs


def primed_sample(x, labels, sampling_kwargs, priors, hps):
    """Prompt the model with raw audio input (dimension: NTC) and generate continuations"""
    sample_levels = list(range(len(priors)))
    zs = priors[-1].encode(x, start_level=0, end_level=len(priors), bs_chunks=x.shape[0])
    zs = _sample(zs, labels, sampling_kwargs, priors, sample_levels, hps)
    return zs


def load_prompts(audio_files, duration, hps):
    """Load `duration` seconds of the given audio files to use as prompts"""
    xs = []
    for audio_file in audio_files:
        x = load_audio(audio_file, sr=hps.sr, duration=duration, offset=0.0, mono=True)
        x = x.T # CT -> TC
        xs.append(x)
    while len(xs) < hps.n_samples:
        xs.extend(xs)
    xs = xs[:hps.n_samples]
    x = t.stack([t.from_numpy(x) for x in xs])
    x = x.to('cuda', non_blocking=True)
    return x


def save_samples(model, device, hps, sample_hps, metas: list):
    """Generate and save samples, alignment, and webpage for visualization."""
    print(hps)
    from jukebox.lyricdict import poems, gpt_2_lyrics
    vqvae, priors = make_model(model, device, hps)

    assert hps.sample_length//priors[-2].raw_to_tokens >= priors[-2].n_ctx, f"Upsampling needs atleast one ctx in get_z_conds. Please choose a longer sample length"
    assert isinstance(metas, list)
    total_length = hps.total_sample_length_in_seconds * hps.sr
    offset = 0
    while len(metas) < hps.n_samples:
        metas.extend(metas)
    metas = metas[:hps.n_samples]

    labels = [prior.labeller.get_batch_labels(metas, 'cuda') for prior in priors]
    for label in labels:
        assert label['y'].shape[0] == hps.n_samples

    lower_level_chunk_size = 32
    lower_level_max_batch_size = 16
    if model == '1b_lyrics':
        chunk_size = 32
        max_batch_size = 16
    else:
        chunk_size = 16
        max_batch_size = 3
    sampling_kwargs = [dict(temp=0.99, fp16=True, chunk_size=lower_level_chunk_size, max_batch_size=lower_level_max_batch_size),
                       dict(temp=0.99, fp16=True, chunk_size=lower_level_chunk_size, max_batch_size=lower_level_max_batch_size),
                       dict(temp=0.99, fp16=True, chunk_size=chunk_size, max_batch_size=max_batch_size)]

    if sample_hps.mode == 'ancestral':
        ancestral_sample(labels, sampling_kwargs, priors, hps)
    elif sample_hps.mode == 'primed':
        assert sample_hps.audio_file is not None
        audio_files = sample_hps.audio_file.split(',')
        top_raw_to_tokens = priors[-1].raw_to_tokens
        duration = (int(sample_hps.prompt_length_in_seconds * hps.sr) // top_raw_to_tokens) * top_raw_to_tokens
        x = load_prompts(audio_files, duration, hps)
        primed_sample(x, labels, sampling_kwargs, priors, hps)
    else:
        raise ValueError(f'Unknown sample mode {sample_hps.mode}.')


def run(mode='ancestral', audio_file=None, prompt_length_in_seconds=12.0, port=29500, **kwargs):
    from jukebox.utils.dist_utils import setup_dist_from_mpi
    from jukebox.utils import queue
    while True:
        # setup distributed communications
        rank, local_rank, device = setup_dist_from_mpi(port=port)
        # connect to db
        db, cur = queue.connectdb()
        offset = 0
        # get the next job
        job = queue.get_next_job(cur)
        queue.closedb(db)
        if job:
            print(job)
            job_id = job['job_id']
            # artist, lyrics, genre
            metas = Hyperparams(dict(artist=job['params']['artist'],
                                     genre=job['params']['genre'],
                                     lyrics=job['params']['lyrics'],
                                     total_length=job['params']['length']*44100,  # remove hardcoded sr
                                     offset=offset))
            kw = dict(**kwargs)
            kw['sample_length_in_seconds'] = int(job['params']['length'])
            kw['total_sample_length_in_seconds'] = int(job['params']['length'])
            kw['n_samples'] = 3 if '5b_lyrics' == job['params']['model'] else 16
            kw['job_id'] = job_id
            kw['name'] = job['params']['name']
            hps = Hyperparams(kw)
            print(hps)
            sample_hps = Hyperparams(dict(mode=mode,
                                          audio_file=audio_file,
                                          prompt_length_in_seconds=prompt_length_in_seconds))
            # Lock the job
            queue.lock(cur, job_id)
            # Start the job
            queue.update_status(cur, job_id, "top_started")
            # Log the URL
            curl = subprocess.Popen(os.path.expanduser('./get_ip.sh'), stdout=subprocess.PIPE)
            ip, _ = curl.communicate()  # (ip, error)
            url = "http://{}/jukebox/{}{}/".format(ip.decode().strip(), job_id, job['params']['name'])

            queue.log(cur,
                      job_id,
                      "URL: http://{}/jukebox/{}{}/".format(ip.decode().strip(), job_id, job['params']['name']))
            # Run the full generating script here
            with t.no_grad():
                save_samples(job['params']['model'], device, hps, sample_hps, [metas])
            # FINISH
            db, cur = queue.connectdb()
            queue.update_status(cur, job_id, "upsampling_done")
            queue.closedb(db)
        else:
            # break the loop
            break

if __name__ == '__main__':
    fire.Fire(run)
