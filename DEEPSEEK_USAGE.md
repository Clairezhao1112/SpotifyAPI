 I prompted AI to “use the DeepSeek API key as shown in the example to include more information on the artist events as shown in the main.py file.” The enrichment strategy that proved most effective was extracting ticket prices and capturing both the minimum and maximum values, since this gave a realistic range of demand. I also added new columns such as a hype score metric, which blended artist popularity from Spotify with the price spread as a proxy for demand, and a time-to-event measure to reflect urgency. Together, these features provided a richer picture of each event’s commercial and fan momentum.

The most challenging part of this part was ensuring that the enrichment acted as a extension of main.py rather than trying to rewrite the existing logic. The solution was to build a modular function that plugged into the existing workflow basically accessing the information from the raw and layering insights on top to reduce the runtime.  

Cost was not a major concern because DeepSeek call only consumes a handful of tokens so even when running on large event sets, the expense remains minimal. In practice, this means the enrichment layer can scale to thousands of events while keeping usage well within a safe and affordable range.

The most creative application I discovered was how DeepSeek could generate compact, human-friendly summaries that integrate directly into the console output. This meant users could quickly scan a clear overview of each event including artist, venue, days till event, hype, and a short description without ever needing to open the original event page. It turned raw data into something immediately useful and digestible, bridging the gap between technical enrichment and user-friendly insights.


