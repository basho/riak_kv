%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at

%%   http://www.apache.org/licenses/LICENSE-2.0

%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.    

%% Constants used by the raw_http resources

%% Names of riak_object metadata fields
-define(MD_CTYPE,    <<"content-type">>).
-define(MD_CHARSET,  <<"charset">>).
-define(MD_ENCODING, <<"content-encoding">>).
-define(MD_VTAG,     <<"X-Riak-VTag">>).
-define(MD_LINKS,    <<"Links">>).
-define(MD_LASTMOD,  <<"X-Riak-Last-Modified">>).
-define(MD_USERMETA, <<"X-Riak-Meta">>).
-define(MD_INDEX,    <<"index">>).
-define(MD_DELETED,  <<"X-Riak-Deleted">>).

%% Names of HTTP header fields
-define(HEAD_CTYPE,           "Content-Type").
-define(HEAD_VCLOCK,          "X-Riak-Vclock").
-define(HEAD_LINK,            "Link").
-define(HEAD_ENCODING,        "Content-Encoding").
-define(HEAD_CLIENT,          "X-Riak-ClientId").
-define(HEAD_USERMETA_PREFIX, "x-riak-meta-").
-define(HEAD_INDEX_PREFIX,    "x-riak-index-").
-define(HEAD_DELETED,         "X-Riak-Deleted").

%% Names of JSON fields in bucket properties
-define(JSON_PROPS,   <<"props">>).
-define(JSON_BUCKETS, <<"buckets">>).
-define(JSON_KEYS,    <<"keys">>).
-define(JSON_LINKFUN, <<"linkfun">>).
-define(JSON_MOD,     <<"mod">>).
-define(JSON_FUN,     <<"fun">>).
-define(JSON_ARG,     <<"arg">>).
-define(JSON_CHASH,   <<"chash_keyfun">>).
-define(JSON_JSFUN,    <<"jsfun">>).
-define(JSON_JSANON,   <<"jsanon">>).
-define(JSON_JSBUCKET, <<"bucket">>).
-define(JSON_JSKEY,    <<"key">>).
-define(JSON_ALLOW_MULT, <<"allow_mult">>).
-define(JSON_EXTRACT, <<"search_extractor">>).
-define(JSON_EXTRACT_LEGACY, <<"rs_extractfun">>).



%% Names of HTTP query parameters
-define(Q_PROPS, "props").
-define(Q_BUCKETS, "buckets").
-define(Q_KEYS,  "keys").
-define(Q_FALSE, "false").
-define(Q_TRUE, "true").
-define(Q_STREAM, "stream").
-define(Q_VTAG,  "vtag").
-define(Q_RETURNBODY, "returnbody").
