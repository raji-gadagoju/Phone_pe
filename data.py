import git

repository_url = "https://github.com/PhonePe/pulse.git"
destination_url = "/Users/upendra/Desktop/Capstone_Projets/PhonePe/data"
git.Repo.clone_from(repository_url,destination_url)

