# tiltIndicatorBefore

Welcome to a lite version of Tilt IndicatorBefore! This repository contains a Python codebase designed to analyze and process tilt data for environmental impact assessments.

### Key Components

#### 1. Data Loading

The repository includes functions to load various input datasets, such as:
- Raw europages companies data
- Scenario targets for different scenarios (IPR and WEO)
- Mapping data for ISIC codes, tilt, and scenarion


#### 2. Scenario Preparation

The code prepares and combines scenario targets for specified years (2030, 2050) based on IPR (1.5C Required Policy Scenario) and WEO (Net Zero Emissions by 2050 Scenario) scenarios.


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
6. Run the fowing command in PowerShell:
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
9. The output of the package will be stored in "/output/"

10. In case you'd like to dive into the coding details, you can install VSCode from:
https://code.visualstudio.com/download

11. Once opened, you can navigate to the cloned repository in your local folder, and open any script.