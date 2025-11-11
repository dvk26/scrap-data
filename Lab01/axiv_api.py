import arxiv

def get_source(arxiv_id, save_dir="./sources"):
    """
    Retrieve the source URL of an academic paper from arXiv using its ID.

    Parameters:
    arxiv_id (str): The arXiv identifier of the paper.

    Returns:
    str: The URL to the source of the paper.
    """
    paper = next(arxiv.Client().results(arxiv.Search(id_list=[arxiv_id])))
    paper.download_source(dirpath=save_dir,
                          filename=f"{arxiv_id.replace('/', '_')}_source.tar.gz")
    

def find_papers(keyword, limit = 5):
    """
    Search for academic papers on arXiv based on a keyword.

    Parameters:
    keyword (str): The search term to look for in paper titles and abstracts.
    limit (int): The maximum number of papers to return.

    Returns:
    list: A list of dictionaries containing paper titles and URLs.
    """
    search = arxiv.Search(
        query=f" all :{ keyword }",
        max_results=limit,
        sort_by=arxiv.SortCriterion.Relevance
    )

    return [ paper . title for paper in arxiv . Client () . results ( search ) ]
# Example usage
titles = find_papers (" food calories ")
print ( titles )

