# [Assetto Corsa Streaming ![GitHub](https://img.shields.io/badge/GitHub-%23121011.svg?logo=github&logoColor=white)](https://github.com/DallasFormulaRacing/assetto_corsa_streaming)

## Basic Information

---

### PM info

* Name - Reid M.
* GitHub - @RMin280
* Discord - [@nerdboi654](https://discord.com/users/810644655393079299)

### Contributors

<a href = "https://github.com/dallasformularacing/assetto_corsa_streaming/graphs/contributors">
  <img src = "https://contrib.rocks/image?repo=dallasformularacing/assetto_corsa_streaming" style="width:auto;height:32px;">
</a>

### Description

Azure Eventhub producer reads `.ld` files, parses them, and sends event data (start, stop, data, error). These events will trigger serverless functions to process and save data produced during runs. This serves as a way to test the data pipeline without the cars.  

The next iteration will be used to stream live telemetry data from Assetto Corsa. This is non-critical, but will be cool for AC competition later in the year. The data processing and analysis will use the same infrastructure, so it can also be used to simulate real testing sessions.  

## Working with the Source

### Dependencies 

* [Python 3.13.X](https://www.python.org/downloads/release/python-3139/) or higher
* [Pipenv](https://pipenv.pypa.io/en/latest/#quick-start) (Install using `pip install pipenv`)
* [Azure Eventhub Client](https://learn.microsoft.com/en-us/python/api/overview/azure/eventhub-readme?view=azure-python) (Install using `pip install azure-eventhub`)  

### Installation

1. Clone the repo `git clone https://github.com/DallasFormulaRacing/assetto_corsa_streaming`
2. Make a new branch in the repo for your changes `git checkout -b <branch-name>`
3. In the root directory of the project, run `pipenv install` to install dependencies and build your virtual environment.
4. Run `pipenv shell` to enter the virtual environment.
5. Create a new directory for any `.ld` files in the current working directory, or locate the path of any files you wish to stream
6. Run `python producer.py` from command line