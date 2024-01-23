echo "Creating virtual environment"
python -m venv ./
echo "Activating virtual environment"
./Scripts/activate.bat
echo "Installing required Python packages"
pip install -r requirements.txt
echo "Running Python script"
python main.py