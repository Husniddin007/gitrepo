from django.db import models

class Owner(models.Model):
    login = models.CharField(max_length=255, db_index=True)

    class Meta:
        unique_together = ("login",)

    def __str__(self):
        return self.login

class Language(models.Model):
    name = models.CharField(max_length=100, db_index=True, unique=True)

    def __str__(self):
        return self.name

class Repo(models.Model):
    owner = models.ForeignKey(Owner, on_delete=models.CASCADE, related_name="repos")
    name = models.CharField(max_length=255)
    name_with_owner = models.CharField(max_length=512, unique=True)
    description = models.TextField(null=True, blank=True)
    stars = models.IntegerField(db_index=True, default=0)
    forks = models.IntegerField(default=0)
    watchers = models.IntegerField(default=0)
    is_fork = models.BooleanField(default=False, db_index=True)
    is_archived = models.BooleanField(default=False, db_index=True)
    disk_usage_kb = models.IntegerField(null=True)
    pull_requests = models.IntegerField(null=True)
    issues = models.IntegerField(null=True)
    primary_language = models.ForeignKey(Language, null=True, blank=True, on_delete=models.SET_NULL, related_name='primary_repos')
    language_count = models.IntegerField(null=True)
    created_at = models.DateTimeField(db_index=True, null=True)
    pushed_at = models.DateTimeField(null=True, db_index=True)
    default_branch_commit_count = models.IntegerField(null=True)
    license = models.CharField(max_length=255, null=True, db_index=True)
    assignable_user_count = models.IntegerField(null=True)
    code_of_conduct = models.CharField(max_length=255, null=True)
    forking_allowed = models.BooleanField(null=True)
    created_year = models.IntegerField(db_index=True, null=True)

    class Meta:
        indexes = [
            models.Index(fields=['created_year', 'primary_language']),
            models.Index(fields=['primary_language', '-stars']),
        ]

    def __str__(self):
        return self.name_with_owner

class RepoLanguage(models.Model):
    repo = models.ForeignKey(Repo, on_delete=models.CASCADE, related_name='repo_languages')
    language = models.ForeignKey(Language, on_delete=models.CASCADE)
    size = models.BigIntegerField(default=0)

    class Meta:
        unique_together = ('repo', 'language')
        indexes = [
            models.Index(fields=['language', 'repo']),
        ]

    def __str__(self):
        return f"{self.repo} - {self.language} ({self.size})"

class Topic(models.Model):
    name = models.CharField(max_length=200, unique=True, db_index=True)

    def __str__(self):
        return self.name

class RepoTopic(models.Model):
    repo = models.ForeignKey(Repo, on_delete=models.CASCADE, related_name='repo_topics')
    topic = models.ForeignKey(Topic, on_delete=models.CASCADE)

    class Meta:
        unique_together = ('repo', 'topic')

    def __str__(self):
        return f"{self.repo} - {self.topic}"
