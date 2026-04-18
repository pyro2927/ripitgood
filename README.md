# Rip It Good

BluRay ripper with MakeMKV and HandBrake integration for Plex-compatible output.

## Setup Instructions

1. **Get an OMDB API Key**
   To use the Rip It Good tool, you need to have an OMDB API key. Follow these steps to obtain one:
   - Go to the [OMDB API website](http://www.omdbapi.com/).
   - Click on the 'API Key' link in the menu.
   - Fill out the required information to register for a free account.
   - Once registered, you'll receive an email with your API key.

2. **Clone the Repository**
   Open your terminal and clone the repository using:
   ```bash
   git clone https://github.com/pyro2927/ripitgood.git
   cd ripitgood
   ```

3. **Install Python Dependencies**
   Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

4. **Install System Dependencies**
   MakeMKV and HandBrake CLI tools are required:
   ```bash
   # Ubuntu/Debian
   sudo apt install makemkv handbrake-cli

   # macOS (with Homebrew)
   brew install --cask makemkv handbrake
   ```

5. **Set Up Environment Variables**
   Create a `.env` file in the root of the project and add your OMDB API key:
   ```
   OMDB_API_KEY=your_api_key_here
   ```

## Usage

Run the ripper:
```bash
python rip.py
```

### Command Line Options

```bash
# Show all options
python rip.py --help

# Specify custom device and output location
python rip.py --device /dev/sr0 --output ~/Movies

# Dry-run to test device detection and metadata lookup
python rip.py --dry-run

# Disable GPU acceleration
python rip.py --no-gpu
```

## Output Structure

Movies are organized in Plex-compatible format:
```
~/Media/Plex/Movies/
  └── Movie Title (Year)/
      └── Movie Title (Year).mkv
```
