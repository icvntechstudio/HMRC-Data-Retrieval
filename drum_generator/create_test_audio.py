import numpy as np
import soundfile as sf
import os

def create_click_track(filename="test_input.wav", duration=10, bpm=120, sr=44100):
    t = np.linspace(0, duration, int(sr * duration), endpoint=False)
    audio = np.zeros_like(t)

    samples_per_beat = int(60 / bpm * sr)

    # Create a short beep (10ms, 1000Hz)
    beep_len = int(0.01 * sr)
    beep_t = np.linspace(0, 0.01, beep_len, endpoint=False)
    beep = np.sin(2 * np.pi * 1000 * beep_t) * 0.5

    for i in range(0, len(audio), samples_per_beat):
        if i + beep_len < len(audio):
            audio[i:i+beep_len] = beep

    sf.write(filename, audio, sr)
    print(f"Created test audio file: {filename}")

if __name__ == "__main__":
    create_click_track()
