# Rip It Good

## Instructions on How to Use the Tool

### Setup Instructions

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
   ```

3. **Install Dependencies**  
   Navigate into the cloned repository and install the required dependencies:
   ```bash
   cd ripitgood
   npm install
   ```

4. **Set Up Environment Variables**  
   Create a `.env` file in the root of the project and add your OMDB API key:
   ```
   OMDB_API_KEY=your_api_key_here
   ```

### Usage

To run the tool, execute the following command in your terminal:
```bash
npm start
```

Enjoy using Rip It Good!