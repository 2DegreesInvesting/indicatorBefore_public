echo "Creating virtual environment"
py -m venv ./
echo "Activating virtual environment"
./Scripts/activate.bat
echo "Installing required Python packages"
py -m pip install -r requirements.txt
echo "Running Python script"
py main.py