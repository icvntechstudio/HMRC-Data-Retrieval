import librosa
import numpy as np
import soundfile as sf
import argparse
import sys
import os

# Ensure we can import synthesizer from the same directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from synthesizer import generate_kick, generate_snare, generate_hihat

def add_sample(track, sample, position):
    # Overlay sample onto track at position
    if position >= len(track):
        return

    end_pos = position + len(sample)
    sample_len = len(sample)

    if end_pos > len(track):
        # Truncate sample if it goes past end of track
        sample = sample[:len(track) - position]
        end_pos = len(track)

    track[position:end_pos] += sample

def generate_drums(input_file, output_file):
    print(f"Loading {input_file}...")
    # Load audio with original sample rate
    y, sr = librosa.load(input_file, sr=None)

    print("Analyzing beat...")
    # Analyze tempo and beats
    tempo, beat_frames = librosa.beat.beat_track(y=y, sr=sr)
    if np.ndim(tempo) > 0:
        tempo = tempo[0]
    print(f"Detected Tempo: {tempo} BPM")

    beat_samples = librosa.frames_to_samples(beat_frames)

    # Generate drum sounds
    kick = generate_kick(sr=sr)
    snare = generate_snare(sr=sr)
    hihat = generate_hihat(sr=sr)

    # Create empty drum track matching input length
    drum_track = np.zeros_like(y)

    # Calculate 8th note duration in samples (approx)
    # tempo is BPM. Beats per second = BPM/60. Seconds per beat = 60/BPM.
    # We use this only for the last beat estimation
    sec_per_beat = 60.0 / tempo
    samples_per_beat = int(sec_per_beat * sr)
    samples_per_eighth = samples_per_beat // 2

    print("Generating drum pattern...")
    # Iterate through detected beats
    for i, beat_sample in enumerate(beat_samples):
        # Determine which beat in the measure (0, 1, 2, 3)
        # Assuming 4/4 starting at 0
        beat_index = i % 4

        # Add drums based on pattern
        if beat_index == 0: # Beat 1
            add_sample(drum_track, kick, beat_sample)
            add_sample(drum_track, hihat, beat_sample)
        elif beat_index == 1: # Beat 2
            add_sample(drum_track, snare, beat_sample)
            add_sample(drum_track, hihat, beat_sample)
        elif beat_index == 2: # Beat 3
            add_sample(drum_track, kick, beat_sample)
            add_sample(drum_track, hihat, beat_sample)
        elif beat_index == 3: # Beat 4
            add_sample(drum_track, snare, beat_sample)
            add_sample(drum_track, hihat, beat_sample)

        # Add off-beat hi-hat (8th note)
        # simple heuristic: add hi-hat halfway to next beat
        if i < len(beat_samples) - 1:
            next_beat = beat_samples[i+1]
            midpoint = (beat_sample + next_beat) // 2
            add_sample(drum_track, hihat, midpoint)
        else:
             # Last beat, estimate position
             add_sample(drum_track, hihat, beat_sample + samples_per_eighth)

    # Normalize output
    if np.max(np.abs(drum_track)) > 0:
        drum_track = drum_track / np.max(np.abs(drum_track)) * 0.9

    # Save output
    sf.write(output_file, drum_track, sr)
    print(f"Generated drum track saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate drums for an audio track.")
    parser.add_argument("input_file", help="Path to input audio file")
    parser.add_argument("output_file", help="Path to save output drum track", default="output_drums.wav", nargs="?")
    args = parser.parse_args()

    generate_drums(args.input_file, args.output_file)
