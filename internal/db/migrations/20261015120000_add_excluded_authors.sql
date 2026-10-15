-- +goose Up
-- Some authors shouldn't be served by the typing game at all, whatever the quote says: leaders
-- and propagandists of genocidal regimes and mass-atrocity movements, far right and far left
-- alike, plus terrorists. Like game_unsuitable, this is a flag, not a deletion — the corpus
-- stays intact.
--
-- excluded_authors holds the list as plain name phrases: lowercase, no punctuation, words separated
-- by single spaces ("hitler", "hermann göring", "kim jong un"). An author matches when its own
-- name, normalized the same way (excluded_author_key), contains the phrase as whole words, so
-- "hitler" matches "Adolf Hitler" but "hermann göring" doesn't match "Albert Göring". Spelling
-- variants are separate rows. Edit the table to change the list, then run
-- SELECT refresh_excluded_authors(); to re-flag rows already saved.
--
-- Plain substring checks rather than regexes on purpose: measured on 2.7M quotes / 150k distinct
-- authors, case-insensitive regex matching took minutes (most of it inside the regex engine),
-- and this migration holds a lock on quotes while it runs.
--
-- A trigger flags rows at write time, so the crawler needs no code or list of its own: any quote
-- whose author matches gets excluded_author = true AND game_unsuitable = true with
-- 'excluded_author' added to unsuitable_reason. The game backend already filters on
-- game_unsuitable = false, so excluded authors drop out of the game with no backend change.

-- excluded_author_key normalizes a name for matching: lowercase, every run of spaces, punctuation
-- and digits turned into one space, padded with a space at both ends so a phrase can be matched as
-- whole words with LIKE '% phrase %'. Separators are spelled out rather than "anything but
-- [:alpha:]", and uppercase accented letters are mapped by hand before lower(): in a C-locale
-- database both [:alpha:] and lower() only know ASCII, so "GÖRING" would otherwise never match.
-- +goose StatementBegin
CREATE FUNCTION excluded_author_key(author TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT ' ' || btrim(regexp_replace(
        lower(translate(author, 'ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÑÒÓÔÕÖØÙÚÛÜÝĂĄĆČĎĘĚŁŃŇŐŒŘŚŞŠȘŢŤȚŮŰŹŻŽ', 'àáâãäåæçèéêëìíîïñòóôõöøùúûüýăąćčďęěłńňőœřśşšșţťțůűźżž')),
        '[[:space:][:punct:][:digit:]–—‘’“”«»„·…]+', ' ', 'g')) || ' '
$$;
-- +goose StatementEnd

CREATE TABLE excluded_authors (
    phrase TEXT PRIMARY KEY CHECK (' ' || phrase || ' ' = excluded_author_key(phrase)),
    label  TEXT NOT NULL,
    reason TEXT NOT NULL
);

ALTER TABLE quotes ADD COLUMN excluded_author BOOLEAN NOT NULL DEFAULT false;

-- +goose StatementBegin
CREATE FUNCTION quote_author_excluded(author TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql STABLE AS $$
DECLARE
    k TEXT;
BEGIN
    IF author IS NULL THEN
        RETURN false;
    END IF;
    k := excluded_author_key(author);
    RETURN EXISTS (SELECT 1 FROM excluded_authors e WHERE k LIKE '% ' || e.phrase || ' %');
END
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
-- reason is left). Matches distinct author names once each rather than every quote, and the
-- flagging update goes through idx_quotes_author. Returns the number of quotes changed.
-- +goose StatementBegin
CREATE FUNCTION refresh_excluded_authors() RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    flagged BIGINT;
    unflagged BIGINT;
BEGIN
    CREATE TEMP TABLE excluded_names ON COMMIT DROP AS
        WITH names AS MATERIALIZED (
            SELECT author, excluded_author_key(author) AS k
            FROM (SELECT DISTINCT author FROM quotes WHERE author IS NOT NULL) a
        )
        SELECT DISTINCT n.author FROM names n JOIN excluded_authors e ON n.k LIKE '% ' || e.phrase || ' %';
    ANALYZE excluded_names;

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

INSERT INTO excluded_authors (phrase, label, reason) VALUES
    -- Nazi Germany
    ('hitler', 'Adolf Hitler', 'nazi'),
    ('goebbels', 'Joseph Goebbels', 'nazi'),
    ('himmler', 'Heinrich Himmler', 'nazi'),
    ('hermann göring', 'Hermann Göring', 'nazi'),
    ('hermann goering', 'Hermann Göring', 'nazi'),
    ('hermann goring', 'Hermann Göring', 'nazi'),
    ('heydrich', 'Reinhard Heydrich', 'nazi'),
    ('rudolf hess', 'Rudolf Hess', 'nazi'),
    ('rudolf heß', 'Rudolf Hess', 'nazi'),
    ('rudolf höss', 'Rudolf Höss', 'nazi'),
    ('rudolf hoess', 'Rudolf Höss', 'nazi'),
    ('rudolf höß', 'Rudolf Höss', 'nazi'),
    ('eichmann', 'Adolf Eichmann', 'nazi'),
    ('mengele', 'Josef Mengele', 'nazi'),
    ('martin bormann', 'Martin Bormann', 'nazi'),
    ('julius streicher', 'Julius Streicher', 'nazi'),
    ('alfred rosenberg', 'Alfred Rosenberg', 'nazi'),
    ('ernst röhm', 'Ernst Röhm', 'nazi'),
    ('ernst roehm', 'Ernst Röhm', 'nazi'),
    -- Other fascist and collaborationist leaders
    ('mussolini', 'Benito Mussolini', 'fascist'),
    ('ante pavelić', 'Ante Pavelić', 'fascist'),
    ('ante pavelic', 'Ante Pavelić', 'fascist'),
    ('ion antonescu', 'Ion Antonescu', 'fascist'),
    ('corneliu codreanu', 'Corneliu Zelea Codreanu', 'fascist'),
    ('corneliu zelea codreanu', 'Corneliu Zelea Codreanu', 'fascist'),
    ('ferenc szálasi', 'Ferenc Szálasi', 'fascist'),
    ('ferenc szalasi', 'Ferenc Szálasi', 'fascist'),
    ('quisling', 'Vidkun Quisling', 'fascist'),
    ('oswald mosley', 'Oswald Mosley', 'fascist'),
    -- Neo-Nazis
    ('george lincoln rockwell', 'George Lincoln Rockwell', 'neo_nazi'),
    ('william luther pierce', 'William Luther Pierce', 'neo_nazi'),
    -- Far-right terrorists
    ('breivik', 'Anders Behring Breivik', 'far_right_terrorist'),
    ('brenton tarrant', 'Brenton Tarrant', 'far_right_terrorist'),
    ('dylann roof', 'Dylann Roof', 'far_right_terrorist'),
    -- Communist dictators and their terror apparatus
    ('stalin', 'Joseph Stalin', 'communist_dictator'),
    ('staline', 'Joseph Stalin', 'communist_dictator'),
    ('stalina', 'Joseph Stalin', 'communist_dictator'),
    ('lavrentiy beria', 'Lavrentiy Beria', 'communist_dictator'),
    ('lavrenti beria', 'Lavrentiy Beria', 'communist_dictator'),
    ('lavrentij beria', 'Lavrentiy Beria', 'communist_dictator'),
    ('lawrenti beria', 'Lavrentiy Beria', 'communist_dictator'),
    ('ławrientij beria', 'Lavrentiy Beria', 'communist_dictator'),
    ('nikolai yezhov', 'Nikolai Yezhov', 'communist_dictator'),
    ('nikolai ezhov', 'Nikolai Yezhov', 'communist_dictator'),
    ('nikolaj jeschow', 'Nikolai Yezhov', 'communist_dictator'),
    ('nikolai jeschow', 'Nikolai Yezhov', 'communist_dictator'),
    ('nikołaj jeżow', 'Nikolai Yezhov', 'communist_dictator'),
    ('felix dzerzhinsky', 'Felix Dzerzhinsky', 'communist_dictator'),
    ('feliks dzerzhinsky', 'Felix Dzerzhinsky', 'communist_dictator'),
    ('feliks dzierżyński', 'Felix Dzerzhinsky', 'communist_dictator'),
    ('felix dserschinski', 'Felix Dzerzhinsky', 'communist_dictator'),
    ('félix dzerjinski', 'Felix Dzerzhinsky', 'communist_dictator'),
    ('mao zedong', 'Mao Zedong', 'communist_dictator'),
    ('mao ze dong', 'Mao Zedong', 'communist_dictator'),
    ('mao tse tung', 'Mao Zedong', 'communist_dictator'),
    ('mao tsé toung', 'Mao Zedong', 'communist_dictator'),
    ('mao tse toung', 'Mao Zedong', 'communist_dictator'),
    ('mao tsé tung', 'Mao Zedong', 'communist_dictator'),
    ('pol pot', 'Pol Pot', 'communist_dictator'),
    ('kim il sung', 'Kim Il-sung', 'communist_dictator'),
    ('kim jong il', 'Kim Jong-il', 'communist_dictator'),
    ('kim jong un', 'Kim Jong-un', 'communist_dictator'),
    ('ceaușescu', 'Nicolae Ceaușescu', 'communist_dictator'),
    ('ceauşescu', 'Nicolae Ceaușescu', 'communist_dictator'),
    ('ceausescu', 'Nicolae Ceaușescu', 'communist_dictator'),
    ('enver hoxha', 'Enver Hoxha', 'communist_dictator'),
    -- Far-left terrorists
    ('andreas baader', 'Andreas Baader', 'far_left_terrorist'),
    ('ulrike meinhof', 'Ulrike Meinhof', 'far_left_terrorist'),
    ('gudrun ensslin', 'Gudrun Ensslin', 'far_left_terrorist'),
    ('abimael guzmán', 'Abimael Guzmán', 'far_left_terrorist'),
    ('abimael guzman', 'Abimael Guzmán', 'far_left_terrorist'),
    -- Other terrorists
    ('osama bin laden', 'Osama bin Laden', 'terrorist'),
    ('usama bin laden', 'Osama bin Laden', 'terrorist'),
    ('abu bakr al baghdadi', 'Abu Bakr al-Baghdadi', 'terrorist'),
    -- Other mass-atrocity dictators
    ('idi amin', 'Idi Amin', 'dictator');

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
DROP FUNCTION excluded_author_key(TEXT);
