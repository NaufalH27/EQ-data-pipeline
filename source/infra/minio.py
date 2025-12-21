import io
import numpy as np
from minio import Minio

class MinioClientService:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = False,
    ):
        self._bucket = bucket

        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

        self._ensure_bucket()

    def _ensure_bucket(self):
        if not self.client.bucket_exists(self._bucket):
            self.client.make_bucket(self._bucket)

    def get_client(self) -> Minio:
        return self.client

    def get_bucket(self) -> str:
        return self._bucket

    def upload_npz(
        self,
        object_name: str,
        bucket: str | None = None,
        **arrays,
    ) -> None:
        bucket = bucket or self._bucket

        buffer = io.BytesIO()
        np.savez(buffer, **arrays)
        buffer.seek(0)

        data = buffer.getvalue()

        self.client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/octet-stream",
        )
