import requests

from bs4 import BeautifulSoup

url = "http://redditlist.com/sfw?page="


def generate_subreddit_list(subreddit_list_path: str, to_file: bool):
    subreddit_list = []
    for i in range(5, 13):
        page = requests.get(url + str(i))
        soup = BeautifulSoup(page.content, "html.parser")
        # Top 500~1500 by Subscriber
        sort_by_subscriber = soup.find_all("div", class_="span4 listing")[1]
        subreddits = sort_by_subscriber.find_all(
            "div", attrs={"data-target-filter": "sfw"}, class_="listing-item"
        )
        subreddits = list(map(lambda x: x["data-target-subreddit"], subreddits))
        subreddit_list += subreddits
    assert len(set(subreddit_list)) == 1000

    if to_file:
        with open(subreddit_list_path, "w") as f:
            f.write(",".join(subreddit_list))
    else:
        print(subreddit_list)


if __name__ == "__main__":
    generate_subreddit_list("data/subreddits.csv", True)
