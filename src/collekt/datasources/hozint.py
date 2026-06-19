from collekt.core.datasource import CLIDataSource


class Hozint(CLIDataSource):
    def __init__(self):
        super().__init__(name='hozint', cmd=['hozint-apiclient', 'query'],
                         time_format='%Y-%m-%d')

