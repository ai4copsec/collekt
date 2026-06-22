from collekt.core.datasource import CLIDataSource


class Hozint(CLIDataSource):
    def __init__(self):
        super().__init__(cmd=['hozint-apiclient', 'query', "--output-format", "parquet"],
                         name="hozint",
                         time_format='%Y-%m-%d')

