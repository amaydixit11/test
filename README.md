# Last.fm Data Collection and Enrichment

This repository automatically collects and enriches Last.fm scrobble data using GitHub Actions. The process runs every hour to fetch new scrobbles and enrich them with tags from the Last.fm API.

## Features

- **Automated Collection**: Runs every hour via GitHub Actions
- **Incremental Updates**: Only fetches new scrobbles since the last run
- **Data Enrichment**: Adds tags for tracks, albums, and artists
- **Intelligent Caching**: Minimizes API calls by caching results
- **Comprehensive Logging**: Detailed logs for monitoring and debugging

## Setup

### 1. Repository Secrets

You need to set up the following secrets in your GitHub repository:

- `LASTFM_USERNAME`: Your Last.fm username
- `LASTFM_API_KEY`: Your Last.fm API key (get one at https://www.last.fm/api/account/create)

To add secrets:
1. Go to your repository settings
2. Click on "Secrets and variables" → "Actions"
3. Click "New repository secret"
4. Add each secret with the appropriate name and value

### 2. Repository Structure

The repository will automatically create the following structure:

```
├── .github/
│   └── workflows/
│       └── lastfm-scraper.yml
├── scripts/
│   ├── collect_data.py
│   └── enrich_data.py
├── data/
│   ├── raw/          # Raw CSV files from Last.fm export
│   └── enriched/     # Enriched CSV files with tags
├── cache/
│   └── lastfm_cache.pkl  # Cached API responses
├── logs/
│   ├── collection.log
│   └── enrichment.log
├── requirements.txt
