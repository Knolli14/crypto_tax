from ctax.preprocess.preprocessor import Preprocessor

if __name__ == "__main__":

    filename = str(input("Enter the filename: ")).strip()
    cex = str(input("Enter the CEX: ")).strip()

    Preprocessor.preprocess_file(filename, cex)
    print("File processed")
