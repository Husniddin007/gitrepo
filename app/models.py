from django.db import models

class Language(models.Model):
    name = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.name


class Topic(models.Model):
    name = models.CharField(max_length=100, unique=True)
    stars = models.IntegerField(default=0)

    def __str__(self):
        return self.name


class Repository(models.Model):
    owner = models.CharField(max_length=100)
    name = models.CharField(max_length=100)
    name_with_owner = models.CharField(max_length=200)
    description = models.TextField(null=True, blank=True)
    stars = models.IntegerField()
    forks = models.IntegerField()
    watchers = models.IntegerField()
    issues = models.IntegerField()
    pull_requests = models.IntegerField()
    disk_usage_kb = models.BigIntegerField()
    assignable_user_count = models.IntegerField()
    default_branch_commit_count = models.IntegerField()
    is_fork = models.BooleanField()
    is_archived = models.BooleanField()
    forking_allowed = models.BooleanField(default=True)
    code_of_conduct = models.CharField(max_length=100, null=True, blank=True)
    license = models.CharField(max_length=200, null=True, blank=True)
    created_at = models.DateTimeField()
    pushed_at = models.DateTimeField()
    primary_language = models.ForeignKey('Language', on_delete=models.SET_NULL, null=True, related_name='primary_repos')

    topics = models.ManyToManyField(Topic, related_name='repositories')

    def __str__(self):
        return f"{self.owner}/{self.name}"


class RepositoryLanguage(models.Model):
    repository = models.ForeignKey(Repository, on_delete=models.CASCADE, related_name='repo_languages')
    language = models.ForeignKey(Language, on_delete=models.CASCADE)
    size = models.BigIntegerField()
