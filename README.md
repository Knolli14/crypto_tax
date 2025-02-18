# Ctax
## A Crypto Tac Calculator

### Motivation
I am a blockchain ethusiast and participate a lot in crypto trading. I have accumulated lots of transactions over the last year on centralized but also on decentralised infrastructure. This tool aims to be a local app to help you with your crypto taxes.

### Status
- I started with Bitpanda and Kucoin integration of csv files that have been exported from their apps. Files will be preprocessed and saved as parquet files to prepare for final analysis.

- Implemented Class for Preprocessing going for a more object orientated style. Can handle single files and directories. I'm also now switched to yaml for config management. Next up will be polishing bitpanda and kucoin code base, especially how they handle conversion. Should be splitted more to correct packages.

### Environment
WSL2
I am using python 3.12.7 with a pyenv virtualenv. FOr dependency management i
installed `pip-tools`.

# tbc
