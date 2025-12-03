import json
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import transaction, IntegrityError
from app.models import Owner, Repo, Language, RepoLanguage, Topic, RepoTopic

# batch sizes
BATCH_REPO_LANG = 1000
BATCH_REPO_TOPIC = 1000
BATCH_REPO_UPDATE = 500


def parse_iso(dt_str):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except Exception:
        return None


class Command(BaseCommand):
    help = "Import GitHub repos from JSON file with batching and caching (primary_language as FK)"

    def add_arguments(self, parser):
        parser.add_argument('jsonfile', type=str)

    def handle(self, *args, **options):
        jsonfile = options['jsonfile']
        self.stdout.write(self.style.NOTICE(f"Loading JSON from {jsonfile} ..."))
        with open(jsonfile, 'r', encoding='utf-8') as f:
            data = json.load(f)

        total = len(data)
        self.stdout.write(self.style.NOTICE(f"Parsed {total} repo records"))

        # preload caches
        existing_owners = {o.login: o for o in Owner.objects.all()}
        existing_langs = {l.name: l for l in Language.objects.all()}
        existing_topics = {t.name: t for t in Topic.objects.all()}
        existing_repos = {r.name_with_owner: r for r in Repo.objects.all().only('id', 'name_with_owner')}

        self.stdout.write(self.style.SUCCESS(
            f"Caches loaded: owners={len(existing_owners)}, languages={len(existing_langs)}, topics={len(existing_topics)}, repos={len(existing_repos)}"
        ))

        to_create_repolangs = []
        to_create_reptopics = []
        repos_to_bulk_update = {}
        created_repos = 0
        processed = 0

        for i, item in enumerate(data, start=1):
            processed += 1

            owner_login = item.get('owner') or item.get('owner_login') or ''
            name = item.get('name') or ''
            name_with_owner = item.get('nameWithOwner') or f"{owner_login}/{name}"

            # get or create owner object (cache)
            owner_obj = existing_owners.get(owner_login)
            if not owner_obj:
                try:
                    owner_obj = Owner.objects.create(login=owner_login)
                except IntegrityError:
                    owner_obj = Owner.objects.filter(login=owner_login).first()
                existing_owners[owner_login] = owner_obj

            # get or create repo (cache)
            repo_obj = existing_repos.get(name_with_owner)
            if not repo_obj:
                repo_kwargs = {
                    'owner': owner_obj,
                    'name': name,
                    'name_with_owner': name_with_owner,
                    'description': item.get('description'),
                    'stars': item.get('stars', 0),
                    'forks': item.get('forks', 0),
                    'watchers': item.get('watchers', 0),
                    'issues': item.get('issues', 0),
                    'pull_requests': item.get('pullRequests', 0),
                    'disk_usage_kb': item.get('diskUsageKb', 0),
                    'assignable_user_count': item.get('assignableUserCount', 0),
                    'default_branch_commit_count': item.get('defaultBranchCommitCount', 0),
                    'is_fork': item.get('isFork', False),
                    'is_archived': item.get('isArchived', False),
                    'forking_allowed': item.get('forkingAllowed', True),
                    'code_of_conduct': item.get('codeOfConduct'),
                    'license': item.get('license'),
                    'created_at': parse_iso(item.get('createdAt')),
                    'pushed_at': parse_iso(item.get('pushedAt')),
                    'language_count': item.get('languageCount'),
                    'created_year': None,
                }
                if repo_kwargs['created_at']:
                    repo_kwargs['created_year'] = repo_kwargs['created_at'].year

                try:
                    with transaction.atomic():
                        repo_obj = Repo.objects.create(**repo_kwargs)
                except IntegrityError:
                    repo_obj = Repo.objects.filter(name_with_owner=name_with_owner).first()
                    if not repo_obj:
                        self.stderr.write(self.style.ERROR(f"Failed to create repo {name_with_owner}"))
                        continue

                existing_repos[name_with_owner] = repo_obj
                created_repos += 1
            else:
                # optionally update fields later
                repo_obj = Repo.objects.get(pk=repo_obj.id)  # fetch full instance when updating
            # primary_language: set foreign key using existing_langs
            primary_lang_name = item.get('primaryLanguage')
            if primary_lang_name:
                lang_obj = existing_langs.get(primary_lang_name)
                if not lang_obj:
                    try:
                        lang_obj = Language.objects.create(name=primary_lang_name)
                    except IntegrityError:
                        lang_obj = Language.objects.filter(name=primary_lang_name).first()
                    existing_langs[primary_lang_name] = lang_obj
                if repo_obj.primary_language_id != lang_obj.id:
                    repo_obj.primary_language = lang_obj
                    repos_to_bulk_update[repo_obj.id] = repo_obj

            # languages -> buffer RepoLanguage
            for lang in item.get('languages', []):
                lname = lang.get('name')
                lsize = lang.get('size', 0) or 0
                if not lname:
                    continue
                lang_obj = existing_langs.get(lname)
                if not lang_obj:
                    try:
                        lang_obj = Language.objects.create(name=lname)
                    except IntegrityError:
                        lang_obj = Language.objects.filter(name=lname).first()
                    existing_langs[lname] = lang_obj

                to_create_repolangs.append(RepoLanguage(repo=repo_obj, language=lang_obj, size=lsize))

            # topics -> buffer RepoTopic
            for topic in item.get('topics', []):
                tname = topic.get('name')
                if not tname:
                    continue
                topic_obj = existing_topics.get(tname)
                if not topic_obj:
                    try:
                        topic_obj = Topic.objects.create(name=tname)
                    except IntegrityError:
                        topic_obj = Topic.objects.filter(name=tname).first()
                    existing_topics[tname] = topic_obj

                to_create_reptopics.append(RepoTopic(repo=repo_obj, topic=topic_obj))

            # flush buffers periodically
            if len(to_create_repolangs) >= BATCH_REPO_LANG or len(to_create_reptopics) >= BATCH_REPO_TOPIC or len(repos_to_bulk_update) >= BATCH_REPO_UPDATE:
                self._flush_buffers(to_create_repolangs, to_create_reptopics, list(repos_to_bulk_update.values()))
                to_create_repolangs = []
                to_create_reptopics = []
                repos_to_bulk_update = {}

            if i % 500 == 0:
                self.stdout.write(self.style.NOTICE(f"Processed {i}/{total} items..."))

        # final flush
        if to_create_repolangs or to_create_reptopics or repos_to_bulk_update:
            self._flush_buffers(to_create_repolangs, to_create_reptopics, list(repos_to_bulk_update.values()))

        self.stdout.write(self.style.SUCCESS(f"Import done. created_repos={created_repos}, processed={processed}"))

    def _flush_buffers(self, rl_buffer, rt_buffer, repos_update_list):
        # 1) bulk create repo languages
        if rl_buffer:
            try:
                RepoLanguage.objects.bulk_create(rl_buffer, batch_size=500)
            except Exception:
                for rl in rl_buffer:
                    try:
                        rl.save()
                    except Exception:
                        continue
        # 2) bulk create repo topics
        if rt_buffer:
            try:
                RepoTopic.objects.bulk_create(rt_buffer, batch_size=500)
            except Exception:
                for rt in rt_buffer:
                    try:
                        rt.save()
                    except Exception:
                        continue
        # 3) bulk update repos (primary_language, stars, pushed_at etc.)
        if repos_update_list:
            # dedupe by id
            uniq = {r.id: r for r in repos_update_list}.values()
            try:
                Repo.objects.bulk_update(list(uniq), ['primary_language', 'stars', 'pushed_at'])
            except Exception:
                for r in uniq:
                    try:
                        r.save(update_fields=['primary_language', 'stars', 'pushed_at'])
                    except Exception:
                        continue
