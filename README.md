# getintel
openintel scraper/parser for creating subdomain wordlists</br>
### requirements
- aria2c
- golang version >= 1.24
### install
```
go install github.com/guusec/getintel@latest
```
### usage
**-src** specify source for data (umbrella and tranco confirmed to work)</br>
**-y** specify year</br>
**-m** specify month</br>
**-d** specify day</br>
**-parse** parse all parquet files in current directory and print names in response_name column</br>
**-aria-binary** file location of aria2c binary (optional)</br>
**-rpc** aria2c rpc server address (optional)</br>
**-token** aria2c rpc token (optional for better security)</br>
**-dir** download directory (optional)</br>
### examples
getintel -src umbrella -y 2025 -m 03 -d 14</br>
getintel -src tranco -y 2025 -m 06 -d 01 -token poggers -dir /media/user/downloads</br>
getintel -parse</br>
### remove duplicate domains and trailing periods
```
getintel -parse | awk '!x[$0]++' | sed 's/\.$//g'
```
### install zsh completions
```bash
cd getintel \
mkdir -p ~/.zsh/completions \
mv _getintel ~/.zsh/completions \
chmod a+r ~/.zsh/completions/_getintel \
fpath=($HOME/.zsh/completions $fpath) \
autoload -Uz compinit && compinit 
```
### todo
- support for more sources
- support for day range specification
- support for parsing more columns
- wordlist creation
