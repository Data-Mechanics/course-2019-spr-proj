from scipy.cluster.vq import kmeans


def health_score(row):
    average = (float(row["obesity"]) + float(row["low_phys"]) + float(row["asthma"])) // 3
    # we implement this scale to exagerate weights
    # in the future should implement method to change how
    # we weight
    if average > 20:
        return 100
    elif average > 15:
        return 10
    else:
        return 1


@staticmethod
def distance_score(distance_score, stdev, mean):
    z_score = (distance_score - mean) / (stdev)
    if z_score > 1.5:
        return 100
    elif z_score > .75:
        return 10
    else:
        return 1

def compute_weight()
def compute_kmeans(neighborhood, parcel_repo, statistics_repo)