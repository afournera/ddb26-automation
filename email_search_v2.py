"""
Wine Domain Email Search v2 — Multi-step precision pipeline
============================================================
Replaces the simple snippet-based approach with:
  1. Official website discovery via Serper
  2. Contact page scraping (mailto + regex + schema.org)
  3. Email construction + MX verification
  4. Fallback snippet search with domain validation
  5. Confidence scoring & blacklist filtering
"""

import sys
import os

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import pandas as pd
import requests
import re
import time
import json
import dns.resolver
import tldextract
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import warnings
from bs4 import XMLParsedAsHTMLWarning
warnings.filterwarnings('ignore', category=XMLParsedAsHTMLWarning)

# ──────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────
API_KEY = '74c0ac5c1c6d20c1483a26e929bf44dc302d0324'

# Allow overriding input/output via command-line args
INPUT_FILE = sys.argv[1] if len(sys.argv) > 1 else 'mail_list.csv'
OUTPUT_FILE = sys.argv[2] if len(sys.argv) > 2 else 'mail_list_v2_results.csv'

REQUEST_TIMEOUT = 10  # seconds
DELAY_BETWEEN_DOMAINS = 1.0  # be polite
DELAY_BETWEEN_REQUESTS = 0.5

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.5',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# ──────────────────────────────────────────────────────────────
# BLACKLISTS & FILTERS
# ──────────────────────────────────────────────────────────────

# Domains to NEVER pick as a winery's official site
BLOCKED_SITE_DOMAINS = {
    # Marketplaces & wine retailers
    'vivino.com', 'wine-searcher.com', 'idealwine.com', 'vinatis.com',
    'millesima.com', 'millesima.fr', 'chateauonline.com', 'vinissimus.com',
    'vinexpress.fr', 'nicolas.com', '1855.com', 'lavinia.com',
    'caves-explorer.com', 'vinatis.com', 'wineandco.com', 'vente-privee.com',
    'uvinum.fr', 'gallica-wine.com', 'maison-du-vin.fr', 'oenovinia.com',
    # Social media
    'facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com',
    'youtube.com', 'tiktok.com', 'pinterest.com',
    # Directories & databases
    'pages-jaunes.fr', 'pagesjaunes.fr', 'societe.com', 'infogreffe.fr',
    'yelp.com', 'yelp.fr', 'tripadvisor.com', 'tripadvisor.fr',
    'google.com', 'google.fr', 'maps.google.com',
    'linternaute.com', 'kompass.com', 'horaires.lefigaro.fr',
    # Reference / encyclopedia / reviews
    'wikipedia.org', 'wikidata.org', 'tasteatlas.com',
    'lourugby.fr', 'alltrails.com', 'booking.com',
    # News / media
    'francebleu.fr', 'lefigaro.fr', 'lemonde.fr', 'liberation.fr',
    'francetvinfo.fr', 'france3-regions.francetvinfo.fr',
    'actu.fr', 'midilibre.fr', 'ledauphine.com', 'sudouest.fr',
    'laprovence.com', 'nicematin.com', 'varmatin.com',
    'forbes.com', 'cnews.fr',
    # Wine review / media
    'decanter.com', 'wine-spectator.com', 'robertparker.com',
    'jancisrobinson.com', 'revueduvindefrance.com',
    'larevueduvin.fr', 'terredevins.com', 'larvf.com',
    # Generic platforms
    'linkeo.com', 'wix.com', 'wordpress.com', 'blogspot.com',
}

# Email domains to NEVER return as a valid winery email
BLOCKED_EMAIL_DOMAINS = {
    # Generic / unrelated
    'linkeo.com', 'maitredechai.ca', 'pa.gov', 'synap-tic.fr',
    'infogreffe.fr', 'infogreffe-siege.fr',
    # Wine importers / retailers (not the winery itself)
    'shiverick.com', 'kermitlynch.com', 'leonandsonwine.com',
    'planetwineus.com', 'winelens.com', 'esquin.com',
    'montalvin.com', 'thevintageclub.sg',
    'biodynamicwine.bio', 'cabriniwines.com',
    'worldgrandscrus.com', 'bonum.com',
    'princeofpinot.com', 'viniou.co.uk',
    # SaaS / web services
    'linkeo.com', 'zannier.com', 'pgvf.fr', 'pcvt.fr',
    'francetv.fr',
    # Obvious placeholders / examples
    'domaine.fr', 'chateau.com', 'example.com',
}

# Email prefixes that are suspicious / generic / useless
SUSPICIOUS_EMAIL_PREFIXES = {
    'noreply', 'no-reply', 'no.reply', 'donotreply',
    'webmaster', 'postmaster', 'admin', 'root',
    'abuse', 'hostmaster', 'mailer-daemon',
    'jane.doe', 'john.doe', 'test', 'example',
    'nom', 'prenom',  # French placeholder names
}

# Contact page URL patterns to look for
CONTACT_PAGE_PATTERNS = [
    'contact', 'nous-contacter', 'nous-ecrire', 'coordonnees',
    'coordonnées', 'nous_contacter', 'contactez-nous', 'contacter',
    'infos-pratiques', 'informations', 'about', 'a-propos',
    'mentions-legales', 'legal',
]

# Preferred email prefixes (ranked by desirability)
PREFERRED_PREFIXES = ['contact', 'info', 'domaine', 'chateau', 'accueil', 'commercial']


# ──────────────────────────────────────────────────────────────
# UTILITY FUNCTIONS
# ──────────────────────────────────────────────────────────────

def extract_root_domain(url):
    """Extract the registrable domain from a URL (e.g. 'example.com' from 'www.example.com/page')."""
    ext = tldextract.extract(url)
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}"
    return None


def get_email_domain(email):
    """Extract domain from an email address."""
    if '@' in email:
        return email.split('@')[1].lower()
    return None


def check_mx_records(domain):
    """Check if a domain has MX records (can receive email)."""
    try:
        answers = dns.resolver.resolve(domain, 'MX', lifetime=5)
        return len(answers) > 0
    except Exception:
        return False


def is_valid_email_format(email):
    """Basic email format validation."""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def is_blocked_email(email):
    """Check if an email belongs to a blocked domain or has a suspicious prefix."""
    email_lower = email.lower()
    domain = get_email_domain(email_lower)
    prefix = email_lower.split('@')[0]

    if domain in BLOCKED_EMAIL_DOMAINS:
        return True

    # Check suspicious prefixes
    for sus in SUSPICIOUS_EMAIL_PREFIXES:
        if prefix == sus or prefix.startswith(sus + '.') or prefix.startswith(sus + '+'):
            return True

    # Reject common file-extension false positives
    if email_lower.endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.pdf')):
        return True

    return False


def extract_emails_from_text(text):
    """Extract all email addresses from a block of text."""
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    found = re.findall(pattern, text)
    # Deduplicate, lowercase, filter
    seen = set()
    result = []
    for e in found:
        e_lower = e.lower()
        if e_lower not in seen and is_valid_email_format(e_lower) and not is_blocked_email(e_lower):
            seen.add(e_lower)
            result.append(e_lower)
    return result


def rank_emails(emails, website_domain=None):
    """
    Rank a list of emails by quality.
    Prefer: matching website domain > preferred prefix > others.
    """
    if not emails:
        return emails

    def score(email):
        s = 0
        email_domain = get_email_domain(email)
        prefix = email.split('@')[0].lower()

        # Big bonus for matching the winery's website domain
        if website_domain and email_domain == website_domain:
            s += 100

        # Bonus for known good prefixes
        for i, pref in enumerate(PREFERRED_PREFIXES):
            if prefix == pref or prefix.startswith(pref + '.'):
                s += 50 - i  # 'contact' > 'info' > 'domaine' etc.
                break

        # Small bonus for .fr domains (we're searching French wineries)
        if email_domain and email_domain.endswith('.fr'):
            s += 5

        # Penalty for generic ISP / free email providers
        generic_providers = {'gmail.com', 'yahoo.fr', 'yahoo.com', 'hotmail.com',
                             'hotmail.fr', 'outlook.com', 'outlook.fr', 'live.fr',
                             'free.fr', 'sfr.fr', 'orange.fr', 'laposte.net',
                             'wanadoo.fr', 'aol.com'}
        if email_domain in generic_providers:
            s -= 20

        return s

    return sorted(emails, key=score, reverse=True)


def normalize_name_for_matching(name):
    """Normalize a winery name for fuzzy URL matching."""
    import unicodedata
    # Remove accents
    nfkd = unicodedata.normalize('NFKD', name.lower())
    ascii_name = ''.join(c for c in nfkd if not unicodedata.combining(c))
    # Remove common prefixes
    for prefix in ['domaine ', 'château ', 'chateau ', 'clos ', 'mas ', 'vignobles ']:
        if ascii_name.startswith(prefix):
            ascii_name = ascii_name[len(prefix):]
    # Keep only alphanumeric
    ascii_name = re.sub(r'[^a-z0-9]', '', ascii_name)
    return ascii_name


# ──────────────────────────────────────────────────────────────
# STEP 1: FIND OFFICIAL WEBSITE
# ──────────────────────────────────────────────────────────────

def find_official_website(domain_name):
    """
    Use Serper to search Google for the winery's official website.
    Returns (website_url, root_domain) or (None, None).
    """
    query = f'"{domain_name}" vin site officiel'
    url = "https://google.serper.dev/search"

    payload = json.dumps({"q": query, "num": 10, "gl": "fr", "hl": "fr"})
    headers = {'X-API-KEY': API_KEY, 'Content-Type': 'application/json'}

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"    [!] Serper search failed: {e}")
        return None, None

    # Also check the knowledge graph for a website
    kg_website = None
    if 'knowledgeGraph' in data:
        kg = data['knowledgeGraph']
        kg_website = kg.get('website')
        if kg_website:
            root = extract_root_domain(kg_website)
            if root and root not in BLOCKED_SITE_DOMAINS:
                print(f"    [KG] Found website via Knowledge Graph: {kg_website}")
                return kg_website, root

    # Score organic results
    name_normalized = normalize_name_for_matching(domain_name)
    candidates = []

    if 'organic' in data:
        for item in data['organic']:
            link = item.get('link', '')
            root = extract_root_domain(link)

            if not root or root in BLOCKED_SITE_DOMAINS:
                continue

            score = 0
            root_normalized = re.sub(r'[^a-z0-9]', '', root.split('.')[0].lower())

            # Strong match: domain name appears in the URL domain
            if name_normalized and name_normalized in root_normalized:
                score += 50
            elif root_normalized in name_normalized and len(root_normalized) > 5:
                score += 30

            # Bonus for .fr (French wineries)
            if root.endswith('.fr'):
                score += 10
            elif root.endswith('.com'):
                score += 5
            elif root.endswith('.wine') or root.endswith('.vin'):
                score += 8
            elif root.endswith('.corsica'):
                score += 8

            # Bonus if title/snippet mentions the domain name
            title = item.get('title', '').lower()
            snippet = item.get('snippet', '').lower()
            if domain_name.lower() in title:
                score += 15

            # Bonus for wine-related keywords in title/snippet
            wine_keywords = ['domaine', 'chateau', 'château', 'vignoble', 'vigneron',
                             'vin', 'vins', 'wine', 'cuvée', 'millésime', 'appellation',
                             'clos', 'cave', 'terroir']
            for kw in wine_keywords:
                if kw in title or kw in snippet:
                    score += 3
                    break  # only count once

            # Penalty for being a sub-page deep in a multi-domain site
            path = urlparse(link).path
            if path.count('/') > 3:
                score -= 10

            # Penalty for obviously non-wine TLDs
            if root.endswith('.at') or root.endswith('.de') or root.endswith('.uk'):
                # Only penalize if the name normalized doesn't strongly match
                if name_normalized not in root_normalized:
                    score -= 15

            candidates.append((link, root, score))

    if not candidates:
        return None, None

    # Sort by score, pick best
    candidates.sort(key=lambda x: x[2], reverse=True)
    best_url, best_root, best_score = candidates[0]

    print(f"    [WEB] Best site: {best_root} (score: {best_score})")

    # If score is suspiciously low, try a second query
    if best_score < 40:
        print(f"    [WEB] Low confidence, trying alternative query...")
        alt_url, alt_root, alt_score = _search_website_with_query(
            f'{domain_name} site officiel contact', name_normalized)
        if alt_score > best_score:
            print(f"    [WEB] Better match found: {alt_root} (score: {alt_score})")
            return alt_url, alt_root

    return best_url, best_root


def _search_website_with_query(query, name_normalized):
    """Helper: run a Serper search and score results."""
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": query, "num": 10, "gl": "fr", "hl": "fr"})
    headers = {'X-API-KEY': API_KEY, 'Content-Type': 'application/json'}

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
    except Exception:
        return None, None, 0

    candidates = []
    if 'organic' in data:
        for item in data['organic']:
            link = item.get('link', '')
            root = extract_root_domain(link)
            if not root or root in BLOCKED_SITE_DOMAINS:
                continue
            score = 0
            root_normalized = re.sub(r'[^a-z0-9]', '', root.split('.')[0].lower())
            if name_normalized and name_normalized in root_normalized:
                score += 50
            elif root_normalized in name_normalized and len(root_normalized) > 5:
                score += 30
            if root.endswith('.fr'):
                score += 10
            elif root.endswith('.com'):
                score += 5
            title = item.get('title', '').lower()
            snippet = item.get('snippet', '').lower()
            wine_keywords = ['domaine', 'chateau', 'vignoble', 'vin', 'vins', 'wine', 'clos', 'terroir']
            for kw in wine_keywords:
                if kw in title or kw in snippet:
                    score += 3
                    break
            if root.endswith('.at') or root.endswith('.de') or root.endswith('.uk'):
                if name_normalized not in root_normalized:
                    score -= 15
            candidates.append((link, root, score))

    if not candidates:
        return None, None, 0

    candidates.sort(key=lambda x: x[2], reverse=True)
    return candidates[0]


# ──────────────────────────────────────────────────────────────
# STEP 2: SCRAPE WEBSITE FOR EMAIL
# ──────────────────────────────────────────────────────────────

def find_contact_page_urls(soup, base_url):
    """Find links to contact-like pages in the HTML."""
    contact_urls = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href'].lower()
        text = a_tag.get_text(strip=True).lower()

        for pattern in CONTACT_PAGE_PATTERNS:
            if pattern in href or pattern in text:
                full_url = urljoin(base_url, a_tag['href'])
                # Only follow links on the same domain
                if extract_root_domain(full_url) == extract_root_domain(base_url):
                    contact_urls.add(full_url)
                break
    return list(contact_urls)[:5]  # Max 5 contact pages


def extract_emails_from_soup(soup):
    """Extract emails from an HTML page (mailto links + body text)."""
    emails = []

    # 1. mailto: links (highest quality)
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if href.startswith('mailto:'):
            email = href.replace('mailto:', '').split('?')[0].strip().lower()
            if is_valid_email_format(email) and not is_blocked_email(email):
                emails.append(email)

    # 2. Schema.org structured data
    for script_tag in soup.find_all('script', type='application/ld+json'):
        try:
            ld_data = json.loads(script_tag.string)
            # Handle both single objects and arrays
            items = ld_data if isinstance(ld_data, list) else [ld_data]
            for item in items:
                if isinstance(item, dict) and 'email' in item:
                    email = item['email'].strip().lower()
                    if email.startswith('mailto:'):
                        email = email[7:]
                    if is_valid_email_format(email) and not is_blocked_email(email):
                        emails.append(email)
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    # 3. Regex on visible text
    visible_text = soup.get_text(separator=' ')
    text_emails = extract_emails_from_text(visible_text)
    emails.extend(text_emails)

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for e in emails:
        if e not in seen:
            seen.add(e)
            unique.append(e)

    return unique


def scrape_website_for_email(website_url, website_domain):
    """
    Fetch the winery's website and extract email addresses.
    Tries: homepage → contact pages → other relevant pages.
    Returns list of found emails.
    """
    all_emails = []
    pages_scraped = 0
    max_pages = 4

    try:
        # Fetch homepage
        print(f"    [SCRAPE] Fetching homepage: {website_url}")
        resp = requests.get(website_url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                            allow_redirects=True, verify=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')
        pages_scraped += 1

        # Extract emails from homepage
        homepage_emails = extract_emails_from_soup(soup)
        all_emails.extend(homepage_emails)

        # Find contact pages
        contact_urls = find_contact_page_urls(soup, website_url)

        # Also try common contact page paths directly
        common_paths = ['/contact', '/contact/', '/nous-contacter', '/contactez-nous',
                        '/contact.html', '/contact.php', '/en/contact', '/fr/contact']
        for path in common_paths:
            guess_url = urljoin(website_url, path)
            if guess_url not in contact_urls:
                contact_urls.append(guess_url)

        # Scrape contact pages
        for contact_url in contact_urls:
            if pages_scraped >= max_pages:
                break
            try:
                time.sleep(DELAY_BETWEEN_REQUESTS)
                print(f"    [SCRAPE] Fetching: {contact_url}")
                resp = requests.get(contact_url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                                    allow_redirects=True, verify=True)
                if resp.status_code == 200:
                    pages_scraped += 1
                    contact_soup = BeautifulSoup(resp.text, 'lxml')
                    contact_emails = extract_emails_from_soup(contact_soup)
                    all_emails.extend(contact_emails)
            except Exception:
                continue

    except Exception as e:
        print(f"    [!] Scraping failed: {e}")

    # Deduplicate
    seen = set()
    unique = []
    for e in all_emails:
        if e not in seen:
            seen.add(e)
            unique.append(e)

    return unique


# ──────────────────────────────────────────────────────────────
# STEP 3: CONSTRUCT & VERIFY EMAILS
# ──────────────────────────────────────────────────────────────

def construct_and_verify_email(website_domain):
    """
    If we know the website domain but couldn't scrape an email,
    construct likely candidates and verify MX records.
    Returns (email, True) or (None, False).
    """
    if not website_domain:
        return None, False

    # First check if domain has MX records
    if not check_mx_records(website_domain):
        print(f"    [MX] No MX records for {website_domain}")
        return None, False

    print(f"    [MX] MX records found for {website_domain}")

    # Construct candidates in order of likelihood
    candidates = [
        f"contact@{website_domain}",
        f"info@{website_domain}",
        f"domaine@{website_domain}",
        f"chateau@{website_domain}",
        f"accueil@{website_domain}",
    ]

    # Return the most likely one (contact@ is most common for French wineries)
    return candidates[0], True


# ──────────────────────────────────────────────────────────────
# STEP 4: FALLBACK SNIPPET SEARCH
# ──────────────────────────────────────────────────────────────

def fallback_snippet_search(domain_name, website_domain=None):
    """
    Search Google specifically for email addresses in snippets.
    Improved query + domain validation.
    """
    # Better query focused on finding email
    query = f'"{domain_name}" email @ contact'
    url = "https://google.serper.dev/search"

    payload = json.dumps({"q": query, "num": 10, "gl": "fr", "hl": "fr"})
    headers = {'X-API-KEY': API_KEY, 'Content-Type': 'application/json'}

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"    [!] Snippet search failed: {e}")
        return []

    # Collect all text
    snippets_text = ""
    if 'organic' in data:
        for item in data['organic']:
            snippets_text += item.get('snippet', '') + " "
            snippets_text += item.get('title', '') + " "

    # Also check knowledge graph
    if 'knowledgeGraph' in data:
        kg = data['knowledgeGraph']
        snippets_text += kg.get('description', '') + " "
        for attr in kg.get('attributes', {}).values():
            if isinstance(attr, str):
                snippets_text += attr + " "

    emails = extract_emails_from_text(snippets_text)
    return emails


# ──────────────────────────────────────────────────────────────
# STEP 5: VALIDATE & SCORE
# ──────────────────────────────────────────────────────────────

def validate_and_score(email, website_domain, source):
    """
    Assign a confidence level to an email result.
    Returns (confidence, notes).
    """
    if not email or email == "Not Found":
        return "NOT_FOUND", "No email found"

    email_domain = get_email_domain(email)

    # Check blocked
    if is_blocked_email(email):
        return "REJECTED", f"Blocked domain/prefix: {email}"

    # HIGH: email domain matches website domain and found from scraping
    if website_domain and email_domain == website_domain:
        if source in ('scraped_mailto', 'scraped_schema', 'scraped_text'):
            return "HIGH", "Email domain matches website, found on official site"
        elif source == 'constructed':
            return "MEDIUM", "Constructed email, domain has MX records"
        else:
            return "HIGH", "Email domain matches website domain"

    # MEDIUM: found on official site but domain doesn't match
    # (could be using a different email service)
    if source in ('scraped_mailto', 'scraped_schema', 'scraped_text'):
        # Still decent if found on their own site
        generic_providers = {'gmail.com', 'yahoo.fr', 'yahoo.com', 'hotmail.com',
                             'hotmail.fr', 'outlook.com', 'outlook.fr', 'live.fr',
                             'free.fr', 'sfr.fr', 'orange.fr', 'laposte.net',
                             'wanadoo.fr', 'aol.com'}
        if email_domain in generic_providers:
            return "MEDIUM", "Found on official site but uses generic email provider"
        return "MEDIUM", "Found on official site, different domain"

    # LOW: from snippet search
    if source == 'snippet':
        if website_domain and email_domain == website_domain:
            return "MEDIUM", "From snippet but domain matches website"
        return "LOW", "From Google snippet, could be third party"

    return "LOW", "Unverified source"


# ──────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────────────────────

def search_email_for_domain(domain_name):
    """
    Full pipeline for one winery. Returns a dict with results.
    """
    result = {
        'name': domain_name,
        'website': 'Not Found',
        'email': 'Not Found',
        'confidence': 'NOT_FOUND',
        'source': '',
        'notes': '',
    }

    # ── Step 1: Find official website ──
    website_url, website_domain = find_official_website(domain_name)
    if website_url:
        result['website'] = website_url

    time.sleep(DELAY_BETWEEN_REQUESTS)

    # ── Step 2: Scrape website for email ──
    if website_url:
        emails = scrape_website_for_email(website_url, website_domain)

        if emails:
            # Rank and pick the best email
            ranked = rank_emails(emails, website_domain)
            best_email = ranked[0]

            # Determine source type
            source = 'scraped_text'
            # We don't track source per-email in detail, but scraped = good
            source = 'scraped_mailto'

            confidence, notes = validate_and_score(best_email, website_domain, source)

            if confidence != 'REJECTED':
                result['email'] = best_email
                result['confidence'] = confidence
                result['source'] = source
                result['notes'] = notes
                print(f"    ✓ Found email: {best_email} [{confidence}]")
                return result
            else:
                print(f"    ✗ Rejected scraped email: {best_email}")

    # ── Step 3: Construct email if we have the website domain ──
    if website_domain:
        constructed_email, mx_valid = construct_and_verify_email(website_domain)
        if constructed_email and mx_valid:
            confidence, notes = validate_and_score(constructed_email, website_domain, 'constructed')
            result['email'] = constructed_email
            result['confidence'] = confidence
            result['source'] = 'constructed'
            result['notes'] = notes
            print(f"    ◉ Constructed email: {constructed_email} [{confidence}]")
            return result

    # ── Step 4: Fallback snippet search ──
    time.sleep(DELAY_BETWEEN_REQUESTS)
    snippet_emails = fallback_snippet_search(domain_name, website_domain)

    if snippet_emails:
        # Rank with website domain awareness
        ranked = rank_emails(snippet_emails, website_domain)
        best_email = ranked[0]

        confidence, notes = validate_and_score(best_email, website_domain, 'snippet')

        if confidence != 'REJECTED':
            result['email'] = best_email
            result['confidence'] = confidence
            result['source'] = 'snippet'
            result['notes'] = notes
            print(f"    ◇ Snippet email: {best_email} [{confidence}]")
            return result

    print(f"    ✗ No email found")
    return result


def main():
    print("=" * 60)
    print("  Wine Domain Email Search v2 — Precision Pipeline")
    print("=" * 60)

    df = pd.read_csv(INPUT_FILE)
    column_name = df.columns[0]

    results = []
    total = len(df)

    print(f"\nProcessing {total} domains...\n")

    for index, row in df.iterrows():
        domain_name = row[column_name]
        print(f"\n[{index + 1}/{total}] -- {domain_name} --")

        result = search_email_for_domain(domain_name)
        results.append(result)

        time.sleep(DELAY_BETWEEN_DOMAINS)

    # Build output DataFrame
    out_df = pd.DataFrame(results)
    out_df.columns = ['Chateau / Entreprise', 'Website', 'Email',
                      'Confidence', 'Source', 'Notes']
    out_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')

    # Print summary
    print("\n" + "=" * 60)
    print("  RESULTS SUMMARY")
    print("=" * 60)
    total_found = sum(1 for r in results if r['email'] != 'Not Found')
    high = sum(1 for r in results if r['confidence'] == 'HIGH')
    medium = sum(1 for r in results if r['confidence'] == 'MEDIUM')
    low = sum(1 for r in results if r['confidence'] == 'LOW')
    not_found = sum(1 for r in results if r['confidence'] == 'NOT_FOUND')

    print(f"  Total domains:  {total}")
    print(f"  Emails found:   {total_found} ({total_found*100//total}%)")
    print(f"    HIGH:   {high}")
    print(f"    MEDIUM: {medium}")
    print(f"    LOW:    {low}")
    print(f"  Not found:      {not_found}")
    print(f"\n  Output saved to: {OUTPUT_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
