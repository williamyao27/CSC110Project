"""
Copyright and Usage Information
===============================
This file is Copyright (c) 2021 Allen Uy, Nathaniel Yong, and William Yao.
"""
from collections import Counter
from nltk.corpus import stopwords
import pandas as pd


def remove_stopwords(s: str) -> str:
    """Returns a string where all the stopwords are removed from s

    Preconditions:
        None
    """
    return " ".join(word for word in s.split() if word not in stopwords.words("english"))


def create_words_and_hashtag_df(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns a tuple containing two dataframes for words and hashtags.

    Preconditions:
        - 'full_text' in df.columns
    """
    word_list = []
    hashtag_list = []
    for tweet in df["full_text"]:
        for word in tweet.split():
            word_list.append(word)
            if "#" in word:
                hashtag_list.append(word)

    words = Counter(word_list)
    hashtags = Counter(hashtag_list)

    words_df = pd.DataFrame.from_records(words.most_common(20), columns=["word", "count"])
    hashtag_df = pd.DataFrame.from_records(hashtags.most_common(15), columns=["hashtag", "count"])

    return (words_df, hashtag_df)


if __name__ == "__main__":
    import doctest
    doctest.testmod()

    import python_ta
    python_ta.check_all(config={
        'extra-imports': ['nltk.corpus', 'collections', 'pandas'],
        'allowed-io': [],
        'max-line-length': 100,
        'disable': ['R1705', 'C0200']
    })

    import python_ta.contracts
    python_ta.contracts.check_all_contracts()
