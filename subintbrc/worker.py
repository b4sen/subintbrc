import threading

import _xxinterpchannels as channels
import _xxsubinterpreters as interpreters


class Worker(threading.Thread):

    def __init__(self, task_str: str, main_ch):
        self.interp = interpreters.create()
        self.channel = channels.create()
        self.main_ch = main_ch
        self.task_str = task_str
        super().__init__(target=self.run, daemon=True)

    def run(self):
        interpreters.run_string(
            self.interp,
            self.task_str,
            shared={"channel_id": self.channel, "main_ch": self.main_ch},
        )

    def process_chunk(self, chunk):
        channels.send(self.channel, chunk)

    def request_stop(self):
        channels.send(self.channel, "stop")

    def is_alive(self) -> bool:
        return interpreters.is_running(self.interp) and super().is_alive()
