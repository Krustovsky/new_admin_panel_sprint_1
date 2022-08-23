from django.db import models

import uuid
from django.core.validators import MinValueValidator, MaxValueValidator

from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), null=True, blank=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('Full name'), max_length=255)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('Star')
        verbose_name_plural = _('Stars')


class Filmwork(UUIDMixin, TimeStampedMixin):
    class Type(models.TextChoices):
        MOVIE = 'movie', _('movie')
        TV_SHOW = 'tv_show', _('tv_show')

    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), null=True, blank=True)
    creation_date = models.DateField(_('creation date'), null=True, blank=True)
    rating = models.FloatField(_('rating'), null=True, blank=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)]
                               )
    type = models.CharField(_('type'), max_length=7,
                            choices=Type.choices,
                            )
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    persons = models.ManyToManyField(Person, through='PersonFilmwork')

    def __str__(self):
        return self.title

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('Movie')
        verbose_name_plural = _('Movies')


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE, verbose_name=_('Genre'))
    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):  # Чтобы не было строки с названием объекта TabularInline, как правильно незнаю
        return ''

    class Meta:
        db_table = "content\".\"genre_film_work"
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')
        constraints = [
            models.UniqueConstraint(fields=['film_work_id', 'genre_id'], name='film_work_person_role_idx')
        ]


class PersonFilmwork(UUIDMixin):

    class RoleType(models.TextChoices):
        ACTOR = 'actor', _('actor')
        DIRECTOR = 'director', _('director')
        PRODUCER = 'producer', _('producer')
        WRITER = 'writer', _('writer')

    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE, verbose_name=_('Star'))
    role = models.TextField(_('role'), null=True, choices=RoleType.choices)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        verbose_name = _('Star')
        verbose_name_plural = _('Stars')
        constraints = [
            models.UniqueConstraint(fields=['film_work_id', 'person_id', 'role'], name='film_work_person_idx')
        ]
