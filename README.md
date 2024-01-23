# tiltIndicatorBefore

Welcome to the Tilt IndicatorBefore! This repository contains a Python codebase designed to analyze and process tilt data for environmental impact assessments. The code leverages the `tiltIndicatorBefore` module and various data loading functions to handle input data related to ecoinvent, companies, sectors, and more.

### Key Components

#### 1. Data Loading

The repository includes functions to load various input datasets, such as:
- Raw europages companies data
- Raw ecoinvent input data
- Ecoinvent inputs overview
- Ecoinvent activities IDs
- Ecoinvent activities CO2 emissions data
- Scenario targets for different scenarios (IPR and WEO)
- Mapping data for ISIC codes, tilt, and scenarios
- Sector resolution data
- Input geography filters
- NLP matching data from europages to ecoinvent


#### 2. Processing

The code performs intermediate and final data processing, including:
- Loading and processing of EP (Europages) companies data
- Matching and mapping EP and ecoinvent data
- Generating intermediate outputs for companies and products
- Generating final outputs for activities overview, input data, emissions profile input data, sector profile input data.
- Calculating emissions profiles for ecoinvent products and input products

#### 3. Scenario Preparation

The code prepares and combines scenario targets for specified years (2030, 2050) based on IPR (1.5C Required Policy Scenario) and WEO (Net Zero Emissions by 2050 Scenario) scenarios.

#### 4. Output Generation

The repository provides functionality to generate CSV outputs for various processed dataframes, such as ecoinvent input data, EP companies, mapping data, activities overview, sector profiles, and emissions profiles.

### Getting Started

1. Install python version 3.9.6 (64 bit) here:
https://www.python.org/downloads/release/python-396/

2. Install and open Git Bash from here: 
https://www.git-scm.com/downloads

3. Open git bash and select your preferred location to clone the repository by running:
```` Shell
cd "/your/preferred/location"
````
4. Clone the repository by running the following command in git bash:
```` Shell
git clone https://github.com/2DegreesInvesting/indicatorBefore_public.git
````
5. Open windows powerShell and navigate to the location of the cloned repository by running:
```` Shell
Set-Location -Path "\path\to\cloned\repository"
````
6. Run the following command in PowerShell:
```` Shell
python --version
````
7. If PowerShell returns a specific python version, run the tiltIndicatorBefore package by running:
```` Shell
./setup_and_run.ps1
````
8. If in step 6 PowerShell did not return a python version but raised an error, run the tiltIndicatorBefore package by running the following command:
```` Shell
./setup_and_run_alt.ps1
````
9. The package will run between 5-10 minutes, after which the output of the package will be stored in "/output/"

10. In case you'd like to dive into the coding details, you can install VSCode from:
https://code.visualstudio.com/download

11. Once opened, you can navigate to the cloned repository in your local folder, and open any script.