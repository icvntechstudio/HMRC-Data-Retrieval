import numpy as np
import scipy.signal

def generate_kick(duration=0.5, sr=44100):
    t = np.linspace(0, duration, int(sr * duration), endpoint=False)
    # Frequency sweep from 150Hz to 50Hz
    frequency = np.linspace(150, 50, len(t))
    # Amplitude envelope (decay)
    envelope = np.exp(-10 * t)
    waveform = np.sin(2 * np.pi * frequency * t) * envelope
    return waveform

def generate_snare(duration=0.2, sr=44100):
    t = np.linspace(0, duration, int(sr * duration), endpoint=False)
    # Noise component
    noise = np.random.uniform(-1, 1, len(t))
    noise_envelope = np.exp(-20 * t)
    noise *= noise_envelope

    # Tonal component (drum body)
    frequency = 180
    tonal = np.sin(2 * np.pi * frequency * t) * np.exp(-15 * t)

    # Mix
    waveform = 0.5 * noise + 0.5 * tonal
    return waveform

def generate_hihat(duration=0.1, sr=44100):
    t = np.linspace(0, duration, int(sr * duration), endpoint=False)
    # High frequency noise
    noise = np.random.uniform(-1, 1, len(t))

    # High pass filter
    sos = scipy.signal.butter(10, 8000, 'hp', fs=sr, output='sos')
    filtered_noise = scipy.signal.sosfilt(sos, noise)

    # Short decay
    envelope = np.exp(-60 * t)
    waveform = filtered_noise * envelope
    return waveform

if __name__ == "__main__":
    # Test generation (not saving, just checking for errors)
    k = generate_kick()
    s = generate_snare()
    h = generate_hihat()
    print("Synthesized kick, snare, hihat successfully.")
