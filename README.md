# Wikipedia-Song-Scraper

Hybrid: Wikidata SPARQL (film list) + Wikipedia (soundtrack articles)
=====================================================================
The key fix: most Indian film Wikipedia pages don't contain the song
table directly. Instead they link to a separate soundtrack article
e.g. "Lagaan (soundtrack)" or "Dilwale Dulhania Le Jayenge (soundtrack)".
This scraper checks BOTH the film page and any linked soundtrack article.

Usage:
    python film_songs_fixed.py

Dependencies:
    pip install requests beautifulsoup4 pandas tqdm
