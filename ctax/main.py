import argparse
from ctax.preprocess import ppfuncs as pp

def main():
    funcs = {
        "bitpanda": pp.preprocess_bitpanda_files,
        "kucoin": pp.preprocess_kucoin_files,
     #   "merge": pp.merge_files,
    }

    parser = argparse.ArgumentParser(description="Preprocess CEX data files.")
    parser.add_argument("filenames", nargs="+", type=str, help="The filenames to process")
    parser.add_argument("cex", type=str, help="The CEX (kucoin or bitpanda)")

    args = parser.parse_args()

    filenames = args.filenames
    cex = args.cex

    # Beispielaufruf einer Funktion aus ppfuncs
    func = funcs.get(cex)

    if func:
        for file_name in filenames:
            func(file_name)

    else:
        print(f"CEX '{cex}' is not supported. Choose from {funcs.keys()}")

    return None

if __name__ == "__main__":
    main()
