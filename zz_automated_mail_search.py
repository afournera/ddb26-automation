import pandas as pd
import requests
import re
import time
import json

# --- CONFIGURATION ---
API_KEY = '74c0ac5c1c6d20c1483a26e929bf44dc302d0324'  # Paste your Serper API key here
INPUT_FILE = 'mail_list.csv'
OUTPUT_FILE = 'mail_list_with_emails.csv'

def find_email_via_serper(domain_name):
    """Searches Google via Serper.dev and extracts emails from snippets."""
    query = f'"{domain_name}" contact email OR "maitre de chai" OR "domaine"'
    url = "https://google.serper.dev/search"
    
    payload = json.dumps({
      "q": query,
      "num": 5 # Look at the top 5 Google results
    })
    
    headers = {
      'X-API-KEY': API_KEY,
      'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        search_results = response.json()
        
        # Combine all text snippets from the search results
        snippets_text = ""
        if 'organic' in search_results:
            for item in search_results['organic']:
                snippets_text += item.get('snippet', '') + " "
                
        # Regex to find email addresses
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        emails_found = re.findall(email_pattern, snippets_text)
        
        if emails_found:
            # Return the first valid email found, converted to lowercase
            valid_emails = [e.lower() for e in emails_found if not e.endswith(('.png', '.jpg', '.jpeg', '.gif'))]
            if valid_emails:
                return valid_emails[0]
            
    except Exception as e:
        print(f"Error searching for {domain_name}: {e}")
        
    return "Not Found"

def main():
    print("Loading CSV...")
    # Read the first 5 rows just to test. 
    # REMOVE '.head(5)' ONCE YOU CONFIRM IT WORKS TO RUN THE WHOLE LIST!
    df = pd.read_csv(INPUT_FILE) 
    
    column_name = df.columns[0] 
    emails = []
    
    print(f"Processing {len(df)} domains...")
    
    for index, row in df.iterrows():
        domain = row[column_name]
        print(f"[{index + 1}/{len(df)}] Searching for: {domain}")
        
        email = find_email_via_serper(domain)
        emails.append(email)
        
        # Small pause to be polite to the API
        time.sleep(0.5) 
        
    df['Email'] = emails
    
    # Save the updated list
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nFinished! Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()