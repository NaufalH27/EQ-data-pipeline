import tensorflow as tf
import numpy as np

def predict(model, seismic_traces, batch_size=32):
    valid_traces = []
    invalid_indices = []

    for i, trace in enumerate(seismic_traces):
        if trace.shape == (6000, 3):
            valid_traces.append(trace)
        else:
            invalid_indices.append(i)

    if len(valid_traces) == 0:
        raise ValueError("No valid traces with shape (6000, 3) found.")

    valid_traces = np.array(valid_traces)
    std = np.std(valid_traces, axis=(1, 2), keepdims=True)

    valid_traces = valid_traces / (std + 1e-8)
    N = valid_traces.shape[0]
    DD_list, PP_list, SS_list = [], [], []

    for i in range(0, N, batch_size):
        batch = valid_traces[i:i+batch_size]
        batch = tf.convert_to_tensor(batch, dtype=tf.float32)
        det, p, s = model(batch, training=False)
        DD_list.append(det.numpy()[..., 0])
        PP_list.append(p.numpy()[..., 0])
        SS_list.append(s.numpy()[..., 0])

    print(f"Skipped traces at indices: {invalid_indices}") 
    return {
        "DD_mean": np.concatenate(DD_list, axis=0),
        "PP_mean": np.concatenate(PP_list, axis=0),
        "SS_mean": np.concatenate(SS_list, axis=0),
        "valid_count": N,
        "skipped_indices": invalid_indices
    }

