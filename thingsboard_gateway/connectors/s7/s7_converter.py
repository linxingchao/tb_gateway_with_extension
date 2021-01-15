from thingsboard_gateway.connectors.converter import Converter, abstractmethod, log


class S7Converter(Converter):
    @abstractmethod
    def convert(self, config, data):
        pass