from django.contrib import admin
from app.models import Language,Topic,RepoLanguage,Repo,Owner,RepoTopic



admin.site.register(Language)
admin.site.register(Topic)
admin.site.register(RepoLanguage)
admin.site.register(Repo)
admin.site.register(Owner)
admin.site.register(RepoTopic)