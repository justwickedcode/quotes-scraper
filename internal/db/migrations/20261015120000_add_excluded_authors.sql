-- +goose Up
-- Some authors shouldn't be served by the typing game at all, whatever the quote says: leaders
-- and propagandists of genocidal regimes and mass-atrocity movements, far right and far left
-- alike, plus terrorists. Like game_unsuitable, this is a flag, not a deletion — the corpus
-- stays intact.
--
-- excluded_authors holds the list. Each pattern is a case-insensitive Postgres regex matched
-- against quotes.author as a whole phrase (not touching other letters on either side), so
-- 'hitler' matches "Adolf Hitler" but 'beria' wouldn't match "Iberia". Edit the table to change
-- the list, then run SELECT refresh_excluded_authors(); to re-flag rows already saved.
--
-- A trigger flags rows at write time, so the crawler needs no code or list of its own: any quote
-- whose author matches gets excluded_author = true AND game_unsuitable = true with
-- 'excluded_author' added to unsuitable_reason. The game backend already filters on
-- game_unsuitable = false, so excluded authors drop out of the game with no backend change.
CREATE TABLE excluded_authors (
    pattern TEXT PRIMARY KEY,
    label   TEXT NOT NULL,
    reason  TEXT NOT NULL
);

ALTER TABLE quotes ADD COLUMN excluded_author BOOLEAN NOT NULL DEFAULT false;

-- +goose StatementBegin
CREATE FUNCTION quote_author_excluded(author TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT author IS NOT NULL AND EXISTS (
        SELECT 1 FROM excluded_authors e
        WHERE author ~* ('(^|[^[:alpha:]])(' || e.pattern || ')($|[^[:alpha:]])')
    )
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION add_unsuitable_reason(reasons TEXT, reason TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN reasons IS NULL OR reasons = '' THEN reason
        WHEN reason = ANY (string_to_array(reasons, ',')) THEN reasons
        ELSE reasons || ',' || reason
    END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION remove_unsuitable_reason(reasons TEXT, reason TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT NULLIF(array_to_string(array_remove(string_to_array(reasons, ','), reason), ','), '')
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION quotes_flag_excluded_author() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.excluded_author := quote_author_excluded(NEW.author);
    IF NEW.excluded_author THEN
        NEW.game_unsuitable := true;
        NEW.unsuitable_reason := add_unsuitable_reason(NEW.unsuitable_reason, 'excluded_author');
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

-- Also on updates to the suitability columns, so cmd/backfill (which recomputes them from the
-- text alone) can't clear an excluded author's flag.
CREATE TRIGGER quotes_flag_excluded_author
    BEFORE INSERT OR UPDATE OF author, game_unsuitable, unsuitable_reason ON quotes
    FOR EACH ROW EXECUTE FUNCTION quotes_flag_excluded_author();

-- refresh_excluded_authors re-applies the current list to every saved quote, flagging new matches
-- and un-flagging authors removed from the list (game_unsuitable stays true only if another
-- reason is left). Works on distinct author names first, so the regexes run once per author
-- rather than once per quote, and the updates go through idx_quotes_author. Returns the number of
-- quotes changed.
-- +goose StatementBegin
CREATE FUNCTION refresh_excluded_authors() RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    flagged BIGINT;
    unflagged BIGINT;
BEGIN
    CREATE TEMP TABLE excluded_names ON COMMIT DROP AS
        SELECT a.author FROM (SELECT DISTINCT author FROM quotes WHERE author IS NOT NULL) a
        WHERE quote_author_excluded(a.author);

    UPDATE quotes q SET
        excluded_author = true,
        game_unsuitable = true,
        unsuitable_reason = add_unsuitable_reason(q.unsuitable_reason, 'excluded_author')
    WHERE q.author IN (SELECT author FROM excluded_names) AND NOT q.excluded_author;
    GET DIAGNOSTICS flagged = ROW_COUNT;

    UPDATE quotes q SET
        excluded_author = false,
        unsuitable_reason = remove_unsuitable_reason(q.unsuitable_reason, 'excluded_author'),
        game_unsuitable = remove_unsuitable_reason(q.unsuitable_reason, 'excluded_author') IS NOT NULL
    WHERE q.excluded_author AND q.author NOT IN (SELECT author FROM excluded_names);
    GET DIAGNOSTICS unflagged = ROW_COUNT;

    DROP TABLE excluded_names;
    RETURN flagged + unflagged;
END
$$;
-- +goose StatementEnd

INSERT INTO excluded_authors (pattern, label, reason) VALUES
    -- Nazi Germany
    ('hitler',                                   'Adolf Hitler',             'nazi'),
    ('goebbels',                                 'Joseph Goebbels',          'nazi'),
    ('himmler',                                  'Heinrich Himmler',         'nazi'),
    ('hermann g(ö|oe|o)ring',                    'Hermann Göring',           'nazi'),
    ('heydrich',                                 'Reinhard Heydrich',        'nazi'),
    ('rudolf he(ss|ß)',                          'Rudolf Hess',              'nazi'),
    ('rudolf h(ö|oe)(ss|ß)',                     'Rudolf Höss',              'nazi'),
    ('eichmann',                                 'Adolf Eichmann',           'nazi'),
    ('mengele',                                  'Josef Mengele',            'nazi'),
    ('martin bormann',                           'Martin Bormann',           'nazi'),
    ('julius streicher',                         'Julius Streicher',         'nazi'),
    ('alfred rosenberg',                         'Alfred Rosenberg',         'nazi'),
    ('ernst r(ö|oe)hm',                          'Ernst Röhm',               'nazi'),
    -- Other fascist and collaborationist leaders
    ('mussolini',                                'Benito Mussolini',         'fascist'),
    ('ante paveli(ć|c)',                         'Ante Pavelić',             'fascist'),
    ('ion antonescu',                            'Ion Antonescu',            'fascist'),
    ('corneliu (zelea )?codreanu',               'Corneliu Zelea Codreanu',  'fascist'),
    ('ferenc sz(á|a)lasi',                       'Ferenc Szálasi',           'fascist'),
    ('quisling',                                 'Vidkun Quisling',          'fascist'),
    ('oswald mosley',                            'Oswald Mosley',            'fascist'),
    -- Neo-Nazis and far-right terrorists
    ('george lincoln rockwell',                  'George Lincoln Rockwell',  'neo_nazi'),
    ('william luther pierce',                    'William Luther Pierce',    'neo_nazi'),
    ('breivik',                                  'Anders Behring Breivik',   'far_right_terrorist'),
    ('brenton tarrant',                          'Brenton Tarrant',          'far_right_terrorist'),
    ('dylann roof',                              'Dylann Roof',              'far_right_terrorist'),
    -- Communist dictators and their terror apparatus
    ('stalin(e|a)?',                             'Joseph Stalin',            'communist_dictator'),
    ('lavrenti[a-z]* beria',                     'Lavrentiy Beria',          'communist_dictator'),
    ('nikola[ij] (yezhov|ezhov|jeschow)',        'Nikolai Yezhov',           'communist_dictator'),
    ('f[eé]li[kx]s? (dzerzhinsk[iy]|dzier(ż|z)y(ń|n)ski|dserschinski)', 'Felix Dzerzhinsky', 'communist_dictator'),
    ('mao (ze[- ]?dong|ts[eé][- ]?t(o)?ung)',    'Mao Zedong',               'communist_dictator'),
    ('pol pot',                                  'Pol Pot',                  'communist_dictator'),
    ('kim il[- ]?sung',                          'Kim Il-sung',              'communist_dictator'),
    ('kim jong[- ]?(il|un)',                     'Kim Jong-il / Kim Jong-un','communist_dictator'),
    ('ceau(ș|ş|s)escu',                          'Nicolae Ceaușescu',        'communist_dictator'),
    ('enver hoxha',                              'Enver Hoxha',              'communist_dictator'),
    -- Far-left terrorists
    ('andreas baader',                           'Andreas Baader',           'far_left_terrorist'),
    ('ulrike meinhof',                           'Ulrike Meinhof',           'far_left_terrorist'),
    ('gudrun ensslin',                           'Gudrun Ensslin',           'far_left_terrorist'),
    ('abimael guzm(á|a)n',                       'Abimael Guzmán',           'far_left_terrorist'),
    -- Other mass-atrocity leaders and terrorists
    ('(osama|usama) bin laden',                  'Osama bin Laden',          'terrorist'),
    ('abu bakr al-baghdadi',                     'Abu Bakr al-Baghdadi',     'terrorist'),
    ('idi amin',                                 'Idi Amin',                 'dictator');

SELECT refresh_excluded_authors();

-- +goose Down
DROP TRIGGER quotes_flag_excluded_author ON quotes;
UPDATE quotes SET
    unsuitable_reason = remove_unsuitable_reason(unsuitable_reason, 'excluded_author'),
    game_unsuitable = remove_unsuitable_reason(unsuitable_reason, 'excluded_author') IS NOT NULL
WHERE excluded_author;
DROP FUNCTION refresh_excluded_authors();
DROP FUNCTION quotes_flag_excluded_author();
DROP FUNCTION remove_unsuitable_reason(TEXT, TEXT);
DROP FUNCTION add_unsuitable_reason(TEXT, TEXT);
DROP FUNCTION quote_author_excluded(TEXT);
ALTER TABLE quotes DROP COLUMN excluded_author;
DROP TABLE excluded_authors;
