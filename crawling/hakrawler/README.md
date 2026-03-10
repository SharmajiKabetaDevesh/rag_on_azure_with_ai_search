# Hakrawler

## Recent Modifications

The following enhancements were made to `hakrawler` to improve performance, support proxy rotation, and provide more versatile output formats:

1. **Concurrency Support**: Instead of processing input URLs sequentially from `stdin`, `hakrawler` can now process multiple input domains simultaneously using goroutines and a WaitGroup.
2. **Multiple Proxies**: Added support for proxy rotation. You can now provide a comma-separated list of proxies, and `hakrawler` will use a Round-Robin strategy to rotate through them for its requests.
3. **CSV Output Formatting**: You can now format the output directly into CSV format (`Source,URL,Where`).
4. **Direct File Output**: Results can now be written directly to a specified output file, avoiding the need to pipe `stdout` or mix output streams.

## New Parameters

- `-it <int>`
  - **Description**: Number of concurrent `stdin` input URLs to process.
  - **Default**: `1`
  - **Example**: `-it 5` (Processes up to 5 domains from stdin simultaneously)

- `-proxies <string>`
  - **Description**: Comma-separated list of Proxy URLs for round-robin proxy rotation.
  - **Example**: `-proxies http://proxy1.example.com:8080,http://proxy2.example.com:8080`

- `-csv`
  - **Description**: Formats the output as a CSV (Comma-Separated Values). The columns are `Source, URL, Where`.
  - **Example**: `-csv`

- `-out <filename>`
  - **Description**: Output file to write the results directly to. If not provided, it falls back to printing to the terminal (`stdout`).
  - **Example**: `-out results.csv` or `-out results.json`

## Example Usage

Combine the new flags to maximize crawling efficiency and clean data output:

```bash
# Crawl multiple domains concurrently through rotating proxies and save to a CSV file
cat domains.txt | ./hakrawler.exe -it 5 -proxies http://proxy1:8080,http://proxy2:8080 -csv -out final_results.csv
```
