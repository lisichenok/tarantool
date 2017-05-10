local ffi   = require('ffi')
local fun   = require('fun')
local errno = require('errno')

-- GID_T, UID_T and TIME_T are, essentially, `integer types`.
-- http://pubs.opengroup.org/onlinepubs/009695399/basedefs/sys/types.h.html
ffi.cdef[[
    typedef int uid_t;
    typedef int gid_t;
    typedef int time_t;
]]

-- POSIX demands to have three fields in struct group:
-- http://pubs.opengroup.org/onlinepubs/009695399/basedefs/grp.h.html
-- char   *gr_name The name of the group.
-- gid_t   gr_gid  Numerical group ID.
-- char  **gr_mem  Pointer to a null-terminated array of character pointers to
--                 member names.
--
-- So we'll extract only them.
ffi.cdef[[
    struct group {
        char    *gr_name;    /* group name */
        char    *gr_passwd;  /* group password */
        gid_t    gr_gid;     /* group id */
        char   **gr_mem;     /* group members */
    };
]]

-- POSIX demands to have five fields in struct group:
-- char    *pw_name   User's login name.
-- uid_t    pw_uid    Numerical user ID.
-- gid_t    pw_gid    Numerical group ID.
-- char    *pw_dir    Initial working directory.
-- char    *pw_shell  Program to use as shell.
--
-- So we'll extract only them.
if ffi.os == 'OSX' or ffi.os == 'BSD' then
    ffi.cdef[[
        struct passwd {
            char    *pw_name;    /* user name */
            char    *pw_passwd;  /* encrypted password */
            uid_t    pw_uid;     /* user uid */
            gid_t    pw_gid;     /* user gid */
            time_t   pw_change;  /* password change time */
            char    *pw_class;   /* user access class */
            char    *pw_gecos;   /* Honeywell login info */
            char    *pw_dir;     /* home directory */
            char    *pw_shell;   /* default shell */
            time_t   pw_expire;  /* account expiration */
            int      pw_fields;  /* internal: fields filled in */
        };
    ]]
else
    ffi.cdef[[
        struct passwd {
            char *pw_name;   /* username */
            char *pw_passwd; /* user password */
            int   pw_uid;    /* user ID */
            int   pw_gid;    /* group ID */
            char *pw_gecos;  /* user information */
            char *pw_dir;    /* home directory */
            char *pw_shell;  /* shell program */
        };
    ]]
end

ffi.cdef[[
    uid_t          getuid();
    struct passwd *getpwuid(uid_t uid);
    struct passwd *getpwnam(const char *login);
    void           endpwent();
    struct passwd *getpwent();
    void           setpwent();

    gid_t          getgid();
    struct group  *getgrgid(gid_t gid);
    struct group  *getgrnam(const char *group);
    struct group  *getgrent();
    void           endgrent();
    void           setgrent();
]]

-- wrappers around getpwuid/getpwnam (automatic type detection)
local function getpw_w(uid)
    local pw = nil
    errno(0)
    if type(uid) == 'number' then
        pw = ffi.C.getpwuid(uid)
    elseif type(uid) == 'string' then
        pw = ffi.C.getpwnam(uid)
    else
        error("Bad type of uid (expected 'string'/'number')", 2)
    end
    return pw
end

-- wrappers around getgruid/getgrnam (automatic type detection)
local function getgr_w(gid)
    local gr = nil
    errno(0)
    if type(gid) == 'number' then
        gr = ffi.C.getgrgid(gid)
    elseif type(gid) == 'string' then
        gr = ffi.C.getgrnam(gid)
    else
        error("Bad type of gid (expected 'string'/'number')", 2)
    end
    return gr
end

local pwgr_errstr = "get%s* failed [errno %d]: %s"

local function getgr_i_gen(group_members, pos)
    local member = group_members[pos]
    if member == nil then
        return nil
    end
    return pos + 1, member
end

-- parses system record and move it into lua table
local function getgr_i(gr)
    local members = fun.iter(getgr_i_gen, gr.gr_mem, 0):map(ffi.string):totable()
    local group = {
        id      = tonumber(gr.gr_gid),
        name    = ffi.string(gr.gr_name),
        members = members,
    }
    return group
end

local function getgr(gid)
    if gid == nil then
        gid = tonumber(ffi.C.getgid())
    end
    local gr = getgr_w(gid)
    if gr == nil then
        if errno() ~= 0 then
            error(pwgr_errstr:format('pw', errno(), errno.strerror()), 2)
        end
        return nil
    end
    return getgr_i(gr)
end

-- parses system record and move it into lua table
local function getpw_i(pw)
    local user = {
        name    = ffi.string(pw.pw_name),
        id      = tonumber(pw.pw_uid),
        group   = getgr(pw.pw_gid),
        workdir = ffi.string(pw.pw_dir),
        shell   = ffi.string(pw.pw_shell),
    }
    return user
end

local function getpw(uid)
    if uid == nil then
        uid = tonumber(ffi.C.getuid())
    end
    local pw = getpw_w(uid)
    if pw == nil then
        if errno() ~= 0 then
            error(pwgr_errstr:format('pw', errno(), errno.strerror()), 2)
        end
        return nil
    end
    return getpw_i(pw)
end

local function getpwall_gen(env, arg)
    local entry = ffi.C.getpwent()
    if entry == nil or errno() ~= 0 then
        return nil
    end
    return arg + 1, entry
end

local function getpwall()
    errno(0)
    -- set to beginning of list
    ffi.C.setpwent()
    if errno() ~= 0 then return nil end
    -- read records one by one
    local pws = fun.iter(getpwall_gen, {}, 1):map(getpw_i):totable()
    if errno() ~= 0 then return nil end
    -- finish processing list of users
    ffi.C.endpwent()
    if errno() ~= 0 then return nil end
    return pws
end

local function getgrall_gen(env, arg)
    local entry = ffi.C.getgrent()
    if entry == nil or errno() ~= 0 then
        return nil
    end
    return arg + 1, entry
end

local function getgrall()
    errno(0)
    -- set to beginning of list
    ffi.C.getgrent()
    if errno() ~= 0 then return nil end
    -- read records one by one
    local grs = fun.iter(getgrall_gen, {}, 1):map(getgr_i):totable()
    if errno() ~= 0 then return nil end
    -- finish processing list of groups
    ffi.C.endgrent()
    if errno() ~= 0 then return nil end
    return grs
end

return {
    getpw = getpw,
    getgr = getgr,
    getpwall = getpwall,
    getgrall = getgrall,
}
