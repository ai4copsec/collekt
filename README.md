# collekt: a library to facilitate spatio-temporal data collection over arbitrary data sources

## Installation

Install the library from source:

```
python -m venv venv-collekt
. venv-collect/bin/activate

git clone https://github.com/ai4copsec/collekt.git
pip install ./collekt
```

## Usage
We are starting to develop the library and interface, so frequent changes might be possible.
However, the following examples show how the commandline interface can be used:

```
$> collekt query --from-time 2026-04-06 --to-time 2026-04-06 --at-lat 50 --at-lon 5 --radius 50 --output-dir all-results
```



If a datasource requires a login, add this information to the .env file.
The following credentials are requires per datasource


### Datasource: HOZINT API
```
HOZINT_APICLIENT_USER=your@email.com
HOZINT_APICLIENT_PASSWORD=yourpassword
HOZINT_APICLIENT_CSRF_TOKEN=yourscsrftoken
```

### Datasource: copernicusmarine
```
COPERNICUSMARINE_SERVICE_USERNAME=your@email.com
COPERNICUSMARINE_SERVICE_PASSWORD=yourcmspassword
```

## Testing

Install the project and use the predefined default test environment:

    pytest tests

## Contributing

This project is open to contributions. For details on how to contribute please check the [Contribution Guidelines](https://github.com/ai4copsec/collekt/blob/main/CONTRIBUTING.md)


## License
This project is licensed under the [BSD-3-Clause License](https://github.com/ai4copsec/collekt/blob/main/LICENSE).

## Copyright

Copyright (c) 2026 [Simula Research Laboratory, Oslo, Norway](https://www.simula.no/research/research-departments)

## Acknowledgments

The development of this library is part of the EU-project [AI4COPSEC](https://ai4copsec.eu) which receives funding from the Horizon Europe framework programme under Grant Agreement N. 101190021.
