import tensorflow as tf
from keras.models import load_model

class EqTransformerService:
    def __init__(
        self,
        model_path: str,
        model_name: str,
        custom_objects: dict,
        gpu_memory_limit: int | None = None,
    ):
        self._configure_gpu(gpu_memory_limit)
        self.model = self._load_model(model_path, custom_objects)
        self.model_name = model_name

    def _configure_gpu(self, gpu_memory_limit: int | None):
        gpus = tf.config.experimental.list_physical_devices("GPU")

        if not gpus:
            print("No GPU found. Using CPU.")
            return

        try:
            if gpu_memory_limit:
                tf.config.experimental.set_virtual_device_configuration(
                    gpus[0],
                    [
                        tf.config.experimental.VirtualDeviceConfiguration(
                            memory_limit=gpu_memory_limit
                        )
                    ],
                )
            print(f"Using GPU: {gpus[0].name}")
        except RuntimeError as e:
            print("GPU configuration error:", e)

    def _load_model(self, model_path: str, custom_objects: dict):
        print("Initializing EqTransformer model")
        model = load_model(
            model_path,
            custom_objects=custom_objects,
        )

        print(f"EqTransformer model loaded ({model_path})")
        return model

