class Payload():
    """ 
    Wrapper for the information exchanged via redis pubsub from client to daemon
    """
    def __init__(self, command, args=None) -> None:
        """
        Args:
            command: str
                String that represents the command to be issued for the daemon
            args:
                arguments that needed to be passed to the command
        """
        self.command = command
        self.args = args