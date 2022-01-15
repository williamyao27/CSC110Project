"""
Copyright and Usage Information
===============================
This file is Copyright (c) 2021 Allen Uy, Nathaniel Yong, and William Yao.
"""
import tweet_data_loader
import tweet_computation
import vaccination_data_loader
import plotly.express as px
import nltk

if __name__ == "__main__":
    # run this the first time
    nltk.download("popular")

    # higher than 23 takes significant time, 23 produces around 30000 tweets
    # lower number takes less time
    data_dirs = ["2020-10", "2020-11", "2020-12", "2021-01", "2021-02", "2021-03", "2021-04"]
    data = tweet_data_loader.read_files(data_dirs, 22)
    df = tweet_data_loader.create_df(data)

    df["full_text"] = df["full_text"].apply(lambda x: tweet_computation.remove_stopwords(x))

    words_df, hashtag_df = tweet_computation.create_words_and_hashtag_df(df)

    hashtags_fig = px.bar(hashtag_df, x="hashtag", y="count")
    hashtags_fig.show()

    words_fig = px.bar(words_df, x="word", y="count")
    words_fig.show()

    # Display vaccination data
    canada_df = vaccination_data_loader.create_df_from_csv("data/vaccination/canada_data.csv")
    vaccination_data_loader.convert_df_types_canada(canada_df)

    cdc_df = vaccination_data_loader.create_df_from_csv("data/vaccination/cdc_data.csv")
    vaccination_data_loader.convert_df_types_cdc(cdc_df)

    total_df = vaccination_data_loader.sum_dfs(canada_df, cdc_df)

    vaccination_fig = px.line(total_df, x="Date", y="Total vaccine doses in Canada and the US combined",
                              title="Total vaccine doses in Canada and the US over time")
    vaccination_fig.show()
