#!/usr/bin/env python
"""
Sample script that uses the QAR_Decode module created using
MATLAB Compiler SDK.

Refer to the MATLAB Compiler SDK documentation for more information.
"""

from __future__ import print_function
import QAR_Decode
import matlab
import os
import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import (
    BlobCheckpointStore,
)
from azure.storage.blob import BlobServiceClient
import io
from dotenv import load_dotenv
import os

class Decoder():

    def __init__(self):
        load_dotenv()
        self.STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")
        self.CONTAINER_NAME = os.getenv("CONTAINER_NAME")
        self.BLOB_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=stevhfdatspdev;AccountKey=0ljyI7fGFrHTeXS1p1BZdDpx1ft2G8Hryo4hDju7bBumZIiOl4K8jbd8GYFoX0IWYrkFaOYegveO+AStQMJiaQ==;EndpointSuffix=core.windows.net"
        self.BLOB_CONTAINER_NAME = "checkpoint-store"
        self.EVENT_HUB_CONNECTION_STR = "Endpoint=sb://evhns-fdatspservices-dev-001.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=d5/7JKwZQuPLnyfsYwSs8sspjgH91m3qW+AEhLED+uU="
        self.EVENT_HUB_NAME = "evh-fdatspservices-dev-001"
        self.client = self.auth()

    def auth(self):
                client = BlobServiceClient.from_connection_string(self.STORAGE_CONNECTION_STRING)
                # [START auth_from_connection_string]
                # [END auth_from_connection_string]
                return client

    async def on_event(self, partition_context, event):
        # Print the event data.
        print(
            'Received the event: "{}" from the partition with ID: "{}"'.format(
                event.body_as_str(encoding="UTF-8"), partition_context.partition_id
            )
        )
        message = event.body_as_str(encoding="UTF-8")

        await self.download_blob_to_file(client, self.CONTAINER_NAME, message)

        my_QAR_Decode = QAR_Decode.initialize()

        absolute_path = os.path.dirname(__file__)
        relative_path = "input"
        QARDirIn = os.path.join(absolute_path, relative_path)

        absolute_path = os.path.dirname(__file__)
        relative_path = "output"
        OutDirIn = os.path.join(absolute_path, relative_path)

        #QARDirIn = "C:\\Users\\xc170f\\source\\repos\\qardecode\\input"
        #OutDirIn = "C:\\Users\\xc170f\\source\\repos\\qardecode\\output"
        airlineIn = "QTR"
        tailNumberIn = "a7bep"

        my_QAR_Decode.QAR_Decode(QARDirIn, OutDirIn, airlineIn, tailNumberIn, nargout=0)

        my_QAR_Decode.terminate()

        # Update the checkpoint so that the program doesn't read the events
        # that it has already read when you run it next time.
        await partition_context.update_checkpoint(event)

    async def download_blob_to_file(self, client, container_name, file_name):
        blob_client = client.get_blob_client(container=container_name, blob=file_name)
        absolute_path = os.path.dirname(__file__)
        relative_path = "input"
        dir_out = os.path.join(absolute_path, relative_path)
        with open(file=os.path.join(dir_out, 'A7-BEP_20220731103615.wgl.zip'), mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())

    async def main(self):
        # Create an Azure blob checkpoint store to store the checkpoints.
        checkpoint_store = BlobCheckpointStore.from_connection_string(
            self.BLOB_STORAGE_CONNECTION_STRING, self.BLOB_CONTAINER_NAME
        )

        # Create a consumer client for the event hub.
        client = EventHubConsumerClient.from_connection_string(
            self.EVENT_HUB_CONNECTION_STR,
            consumer_group="$Default",
            eventhub_name=self.EVENT_HUB_NAME,
            checkpoint_store=checkpoint_store,
        )
        async with client:
            # Call the receive method. Read from the beginning of the
            # partition (starting_position: "-1")
            print("Listening for events...")
            await client.receive(on_event=self.on_event, starting_position="-1")


if __name__ == "__main__":
    decoder = Decoder()
    client = decoder.auth()

    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(decoder.main())

