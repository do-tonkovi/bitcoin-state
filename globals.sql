--
-- PostgreSQL database cluster dump
--

\restrict iECVKpQ01Eob00vgRCBeFkxZla04IG6bz2AzW8KknLQDEGbQU6AroM6ZoCt80CW

SET default_transaction_read_only = off;

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

--
-- Roles
--

CREATE ROLE dotonkovic;
ALTER ROLE dotonkovic WITH NOSUPERUSER INHERIT NOCREATEROLE CREATEDB LOGIN NOREPLICATION NOBYPASSRLS;
CREATE ROLE postgres;
ALTER ROLE postgres WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION BYPASSRLS;

--
-- User Configurations
--








\unrestrict iECVKpQ01Eob00vgRCBeFkxZla04IG6bz2AzW8KknLQDEGbQU6AroM6ZoCt80CW

--
-- PostgreSQL database cluster dump complete
--

