/* contrib/twitter_fdw/twitter_fdw--1.0.sql */

-- create wrapper with validator and handler
CREATE OR REPLACE FUNCTION twitter_fdw_validator (text[], oid)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION twitter_fdw_handler ()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER twitter_fdw
VALIDATOR twitter_fdw_validator HANDLER twitter_fdw_handler;

CREATE SERVER twitter_service FOREIGN DATA WRAPPER twitter_fdw;

CREATE USER MAPPING FOR current_user SERVER twitter_service;

CREATE FOREIGN TABLE twitter(
  id bigint,
  text text,
  from_user text,
  from_user_id bigint,
  to_user text,
  to_user_id bigint,
  iso_language_code text,
  source text,
  profile_image_url text,
  created_at timestamp,
  
  -- virtual columns for parameters
  q text
) SERVER twitter_service;
