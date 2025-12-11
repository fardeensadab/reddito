#!/usr/bin/env python3
"""
Reddit Data Fetcher using Selenium
Fetches posts and comments from Reddit collections with infinite scroll support
"""

import argparse
import json
import os
import time
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager


class RedditFetcher:
    def __init__(self, data_dir: str = "data", community_name: str = None):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.community_name = community_name
        self.community_dir = None
        self.id_tracker_file = None
        self.hash_tracker_file = None
        self.next_id = None
        self.collected_hashes = None
        self.driver = None
        
        # Setup logging
        self.log_file = self.data_dir / "fetch_debug.log"
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("="*80)
        self.logger.info("RedditFetcher initialized")
        self.logger.info(f"Data directory: {self.data_dir.absolute()}")
        self.logger.info(f"Log file: {self.log_file.absolute()}")
    
    def _extract_flair_from_url(self, url: str) -> Optional[str]:
        """
        Extract flair name from URL parameter
        Example: f=flair_name%3A%22Discussion%2F%E0%A6%86%E0%A6%B2%E0%A7%87%E0%A6%BE%E0%A6%9A%E0%A6%A8%E0%A6%BE%22
        Returns: "Discussion" (first part before /)
        """
        import re
        from urllib.parse import unquote
        
        try:
            # Find the f parameter in the URL
            match = re.search(r'[?&]f=([^&]+)', url)
            if match:
                flair_param = unquote(match.group(1))
                # Extract text between quotes: flair_name:"Discussion/আলোচনা"
                flair_match = re.search(r'flair_name:"([^"]+)"', flair_param)
                if flair_match:
                    flair_text = flair_match.group(1)
                    # Get the first part before the slash
                    flair_name = flair_text.split('/')[0].strip()
                    return flair_name
        except Exception as e:
            self.logger.debug(f"Error extracting flair: {e}")
        
        return None
    
    def _setup_community_dir(self, collection_url: str):
        """Setup community-specific directory from URL"""
        # Extract community name from URL (e.g., r/bangladesh)
        import re
        match = re.search(r'/r/([^/?]+)', collection_url)
        if match:
            community_base = f"r_{match.group(1)}"
        else:
            community_base = "r_unknown"
        
        # Extract flair name if available
        flair_name = self._extract_flair_from_url(collection_url)
        
        # Create directory structure: r_community/flair_name or r_community if no flair
        if flair_name:
            self.community_name = f"{community_base}/{flair_name}"
            self.community_dir = self.data_dir / community_base / flair_name
        else:
            self.community_name = community_base
            self.community_dir = self.data_dir / community_base
        
        self.community_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup tracker files for this community
        self.id_tracker_file = self.community_dir / "id_tracker.json"
        self.hash_tracker_file = self.community_dir / "hash_tracker.json"
        self.next_id = self._load_next_id()
        self.collected_hashes = self._load_collected_hashes()
        
    def _load_next_id(self) -> int:
        """Load the next available ID from tracker file"""
        if self.id_tracker_file.exists():
            with open(self.id_tracker_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('next_id', 1)
        return 1
    
    def _save_next_id(self):
        """Save the next available ID to tracker file"""
        with open(self.id_tracker_file, 'w', encoding='utf-8') as f:
            json.dump({'next_id': self.next_id}, f, ensure_ascii=False, indent=2)
    
    def _load_collected_hashes(self) -> Set[str]:
        """Load previously collected post hashes"""
        if self.hash_tracker_file.exists():
            with open(self.hash_tracker_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(data.get('hashes', []))
        return set()
    
    def _save_collected_hashes(self):
        """Save collected hashes to tracker file"""
        with open(self.hash_tracker_file, 'w', encoding='utf-8') as f:
            json.dump({'hashes': list(self.collected_hashes)}, f, ensure_ascii=False, indent=2)
    
    def _generate_hash(self, url: str) -> str:
        """Generate a hash from the post URL"""
        return hashlib.md5(url.encode('utf-8')).hexdigest()
    
    def setup_driver(self):
        """Setup Chrome WebDriver with appropriate options"""
        chrome_options = Options()
        # Uncomment the line below to run headless (without opening browser window)
        # chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Automatically download and setup ChromeDriver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.maximize_window()
        print("✓ Chrome driver initialized")
    
    def scroll_and_wait(self, scroll_pause_time: float = 2.0):
        """Scroll down the page and wait for content to load"""
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_pause_time)
        new_height = self.driver.execute_script("return document.body.scrollHeight")
        return new_height != last_height
    
    def collect_post_links(self, collection_url: str, count: int) -> List[str]:
        """
        Collect post links from the collection page with infinite scroll
        """
        # Setup community directory from URL
        self._setup_community_dir(collection_url)
        
        print(f"Opening collection page: {collection_url}")
        print(f"Community: {self.community_name}")
        print(f"Saving to: {self.community_dir}\n")
        
        self.driver.get(collection_url)
        time.sleep(3)  # Wait for initial page load
        
        # Pause for manual CAPTCHA solving
        print("\n" + "="*80)
        print("⚠️  PLEASE SOLVE ANY CAPTCHA OR VERIFICATION IN THE BROWSER")
        print("="*80)
        input("Press ENTER when you're ready to continue...")
        print("✓ Continuing with scraping...\n")
        
        collected_links = []
        seen_links = set()
        scroll_attempts = 0
        max_scroll_attempts = 50  # Prevent infinite scrolling
        
        print(f"Collecting up to {count} post links...")
        
        while len(collected_links) < count and scroll_attempts < max_scroll_attempts:
            try:
                # Find all post containers (shreddit-post elements)
                post_containers = self.driver.find_elements(By.TAG_NAME, 'shreddit-post')
                
                for container in post_containers:
                    try:
                        # Get the link from within this post container
                        link_element = container.find_element(By.CSS_SELECTOR, 'a[slot="full-post-link"]')
                        link = link_element.get_attribute('href')
                        
                        if link and link not in seen_links:
                            post_hash = self._generate_hash(link)
                            # Skip if already collected
                            if post_hash not in self.collected_hashes:
                                # Don't extract votes here - get them from individual post page
                                collected_links.append({'url': link, 'votes_preview': None})
                                seen_links.add(link)
                                print(f"  Found post #{len(collected_links)}: {link[:80]}...")
                                
                                if len(collected_links) >= count:
                                    break
                    except (StaleElementReferenceException, NoSuchElementException):
                        continue
                
                # If we have enough links, stop
                if len(collected_links) >= count:
                    break
                
                # Scroll down to load more posts
                has_more_content = self.scroll_and_wait()
                scroll_attempts += 1
                
                if not has_more_content:
                    print("  No more content to load")
                    break
                    
            except Exception as e:
                print(f"  Error collecting links: {e}")
                scroll_attempts += 1
        
        print(f"✓ Collected {len(collected_links)} post links")
        return collected_links[:count]
    
    def extract_post_data(self, post_url: str, votes_preview: str = None) -> Optional[Dict]:
        """
        Extract all data from a single post including comments
        """
        self.logger.info(f"="*80)
        self.logger.info(f"Extracting post data from: {post_url}")
        self.logger.info(f"Pre-collected votes: '{votes_preview}'")
        print(f"\nExtracting data from: {post_url}")
        
        try:
            # Open post in new tab
            self.driver.execute_script("window.open('');")
            self.driver.switch_to.window(self.driver.window_handles[-1])
            self.driver.get(post_url)
            time.sleep(3)  # Wait for page load
            
            post_data = {
                'url': post_url,
                'hash': self._generate_hash(post_url),
                'scraped_at': datetime.now().isoformat(),
            }
            
            # Extract title
            try:
                title_element = self.driver.find_element(By.CSS_SELECTOR, 'h1[slot="title"]')
                post_data['title'] = title_element.text.strip()
                print(f"  Title: {post_data['title'][:60]}...")
            except NoSuchElementException:
                post_data['title'] = None
                print("  Title: Not found")
            
            # Extract description/body
            try:
                body_div = self.driver.find_element(By.CSS_SELECTOR, 'div[slot="text-body"]')
                paragraphs = body_div.find_elements(By.TAG_NAME, 'p')
                description = '\n'.join([p.text.strip() for p in paragraphs if p.text.strip()])
                post_data['description'] = description
                print(f"  Description: {len(description)} characters")
            except NoSuchElementException:
                post_data['description'] = None
                print("  Description: Not found")
            
            # Extract votes from the individual post page
            try:
                # Try multiple selectors since Reddit has different layouts
                vote_count = None
                
                # Method 1: seeker-post-info-row (new layout)
                try:
                    vote_element = self.driver.find_element(By.CSS_SELECTOR, 
                        'div[data-testid="seeker-post-info-row"] faceplate-number:first-of-type')
                    vote_count = vote_element.text.strip()
                    if not vote_count:
                        vote_count = vote_element.get_attribute('number')
                    self.logger.info(f"✓ Found votes via seeker-post-info-row: '{vote_count}'")
                except NoSuchElementException:
                    self.logger.debug("seeker-post-info-row not found, trying shreddit-post")
                    
                    # Method 2: Within shreddit-post element
                    try:
                        shreddit_post = self.driver.find_element(By.TAG_NAME, 'shreddit-post')
                        vote_element = shreddit_post.find_element(By.CSS_SELECTOR, 'faceplate-number')
                        vote_count = vote_element.text.strip()
                        if not vote_count:
                            vote_count = vote_element.get_attribute('number')
                        self.logger.info(f"✓ Found votes via shreddit-post: '{vote_count}'")
                    except NoSuchElementException:
                        self.logger.debug("shreddit-post method failed, trying score attribute")
                        
                        # Method 3: Get from shreddit-post score attribute
                        try:
                            shreddit_post = self.driver.find_element(By.TAG_NAME, 'shreddit-post')
                            vote_count = shreddit_post.get_attribute('score')
                            self.logger.info(f"✓ Found votes via score attribute: '{vote_count}'")
                        except:
                            self.logger.error("All vote extraction methods failed")
                
                post_data['votes'] = vote_count
                
            except Exception as e:
                self.logger.error(f"✗ Error extracting votes: {e}")
                post_data['votes'] = votes_preview  # Fallback to preview if available
            
            self.logger.debug(f"Final post_data['votes'] = {post_data.get('votes')}")
            print(f"  Votes: {post_data['votes']}")
            
            # Scroll to load all comments
            print("  Loading comments...")
            for _ in range(5):  # Scroll a few times to load nested comments
                self.scroll_and_wait(1.5)
            
            # Extract comments
            post_data['comments'] = self.extract_comments()
            print(f"  ✓ Extracted {self._count_comments(post_data['comments'])} comments")
            
            # Close the tab and switch back
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
            
            return post_data
            
        except Exception as e:
            print(f"  ✗ Error extracting post data: {e}")
            # Try to close tab and switch back
            try:
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])
            except:
                pass
            return None
    
    def extract_comments(self) -> List[Dict]:
        """
        Extract all comments with nested replies recursively
        """
        try:
            # Find the comment tree
            comment_tree = self.driver.find_element(By.TAG_NAME, 'shreddit-comment-tree')
            
            # Find all top-level comments
            top_level_comments = comment_tree.find_elements(By.XPATH, './shreddit-comment')
            
            comments = []
            for comment_elem in top_level_comments:
                comment_data = self._extract_single_comment(comment_elem)
                if comment_data:
                    comments.append(comment_data)
            
            return comments
            
        except NoSuchElementException:
            return []
        except Exception as e:
            print(f"    Error extracting comments: {e}")
            return []
    
    def _extract_single_comment(self, comment_element) -> Optional[Dict]:
        """
        Extract a single comment with all its nested replies
        """
        try:
            comment_data = {}
            
            # Extract comment text
            try:
                comment_body = comment_element.find_element(By.CSS_SELECTOR, 'div[slot="comment"]')
                paragraphs = comment_body.find_elements(By.TAG_NAME, 'p')
                text = '\n'.join([p.text.strip() for p in paragraphs if p.text.strip()])
                if not text:  # If no paragraphs, get all text
                    text = comment_body.text.strip()
                comment_data['text'] = text
            except NoSuchElementException:
                comment_data['text'] = None
            
            # Extract votes
            try:
                vote_element = comment_element.find_element(By.CSS_SELECTOR, 'faceplate-number')
                vote_text = vote_element.get_attribute('number') or vote_element.text
                comment_data['votes'] = vote_text.strip()
            except NoSuchElementException:
                comment_data['votes'] = None
            
            # Extract nested replies
            comment_data['replies'] = []
            try:
                # Look for nested comments
                nested_comments = comment_element.find_elements(By.XPATH, './shreddit-comment')
                for nested_comment in nested_comments:
                    nested_data = self._extract_single_comment(nested_comment)
                    if nested_data:
                        comment_data['replies'].append(nested_data)
            except Exception:
                pass
            
            return comment_data
            
        except Exception as e:
            return None
    
    def _count_comments(self, comments: List[Dict]) -> int:
        """Count total comments including nested replies"""
        count = len(comments)
        for comment in comments:
            if 'replies' in comment:
                count += self._count_comments(comment['replies'])
        return count
    
    def save_post(self, post_data: Dict) -> int:
        """
        Save post data to JSON file with unique ID
        Returns the ID used
        """
        post_id = self.next_id
        filename = self.community_dir / f"{post_id}.json"
        
        self.logger.info(f"Saving post #{post_id}")
        self.logger.debug(f"Post data keys: {list(post_data.keys())}")
        self.logger.debug(f"Post votes value: '{post_data.get('votes')}'")
        self.logger.debug(f"Post title: {post_data.get('title', 'N/A')[:50]}...")
        
        # Add ID to post data
        post_data['id'] = post_id
        
        # Save to file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(post_data, f, ensure_ascii=False, indent=2)
        
        # Update trackers
        self.collected_hashes.add(post_data['hash'])
        self.next_id += 1
        self._save_next_id()
        self._save_collected_hashes()
        
        print(f"✓ Saved as {filename}")
        return post_id
    
    def fetch_posts(self, collection_url: str, count: int):
        """
        Main method to fetch posts from Reddit collection
        """
        print(f"\n{'='*80}")
        print(f"Reddit Data Fetcher")
        print(f"{'='*80}")
        print(f"Collection URL: {collection_url}")
        print(f"Posts to fetch: {count}")
        print(f"Data directory: {self.data_dir.absolute()}")
        print(f"Next ID: {self.next_id}")
        print(f"{'='*80}\n")
        
        try:
            # Setup browser
            self.setup_driver()
            
            # Collect post links
            post_links = self.collect_post_links(collection_url, count)
            
            if not post_links:
                print("\n✗ No post links found!")
                return
            
            # Extract and save each post
            successful = 0
            failed = 0
            
            for i, post_info in enumerate(post_links, 1):
                print(f"\n[{i}/{len(post_links)}] Processing post...")
                post_data = self.extract_post_data(post_info['url'], post_info.get('votes_preview'))
                
                if post_data:
                    post_id = self.save_post(post_data)
                    successful += 1
                    print(f"✓ Successfully saved post #{post_id}")
                else:
                    failed += 1
                    print(f"✗ Failed to extract post data")
                
                # Small delay between posts
                time.sleep(2)
            
            print(f"\n{'='*80}")
            print(f"Scraping Complete!")
            print(f"{'='*80}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print(f"Total files in {self.community_name}: {len(list(self.community_dir.glob('*.json')))}")
            print(f"{'='*80}\n")
            
        except KeyboardInterrupt:
            print("\n\n✗ Interrupted by user")
        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.driver:
                print("\nClosing browser...")
                self.driver.quit()
                print("✓ Browser closed")


def main():
    parser = argparse.ArgumentParser(
        description='Fetch Reddit posts and comments using Selenium',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fetch.py --from "https://www.reddit.com/r/bangladesh/?f=flair_name%3A%22AskDesh%22" --count 10
  python fetch.py --from "https://www.reddit.com/r/python/top/" --count 5
        """
    )
    
    parser.add_argument(
        '--from',
        dest='collection_url',
        required=True,
        help='Reddit collection URL to scrape'
    )
    
    parser.add_argument(
        '--count',
        type=int,
        required=True,
        help='Number of posts to fetch'
    )
    
    parser.add_argument(
        '--data-dir',
        default='data',
        help='Directory to save JSON files (default: data)'
    )
    
    args = parser.parse_args()
    
    # Validate count
    if args.count <= 0:
        print("Error: --count must be a positive number")
        return 1
    
    # Create fetcher and run
    fetcher = RedditFetcher(data_dir=args.data_dir)
    fetcher.fetch_posts(args.collection_url, args.count)
    
    return 0


if __name__ == '__main__':
    exit(main())
