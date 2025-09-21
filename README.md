This project integrates multiple data sources to give users a complete picture of music events. By entering the name of an artist, the script retrieves albums, song titles, publication dates, links, and popularity scores directly from the Spotify API. It then scrapes Ticketmaster based on the same artist name to gather event information, including location, time, and the URL for purchasing tickets. The purpose is to unify artist and event data into a single pipeline that makes it easier to explore both music content and live performance opportunities.

The DeepSeek API builds on top of this base data by extracting ticket price ranges (minimum and maximum values) and computing additional features. For example, the raw Ticketmaster scrape for Taylor Swift might only return structured details such as artist, venue, city, date, popularity, and a link to buy tickets. After enrichment, the same event hype score that blends artist popularity, ticket price spread as a proxy for demand, and the number of days until the event, as well as a traffic-light style sellout risk indicator.DeepSeek also generates a short, human-friendly description of each event such as ““Taylor Swift's intimate 2011 acoustic set at Phoenix” so that users can scan summaries directly in the console or CSV output without needing to open the event webpage. More example under the examples table. 

To install, first clone the repository:

git clone https://github.com/Clairezhao1112/SpotifyAPI.git
cd SpotifyAPI

Then install dependencies with:

pip install -r requirements.txt

You will need API keys for Spotify and DeepSeek. Set them as environment variables or place them in a .env file:

SPOTIFY_CID=a2fcd932726e496dbfb04cec705edea4

SPOTIFY_SEC=1c46ff7084d945f795227d3e408ad2a8

DEEPSEEK_API_KEY=sk-a7f42564324a433b836f39b479e4dfa8
