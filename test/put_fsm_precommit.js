function precommit_noop(object)
{
    return object;
}

function precommit_append_value(object)
{
    upd_data = object.values[[0]].data + "_precommit_hook_was_here";
    object.values[[0]].data = upd_data;
    return object;
}

function precommit_nonobj(object)
{
    return "not_an_obj";
}

function precommit_fail(object)
{
    return "fail";
}

function precommit_fail_reason(object)
{
    return {"fail":"the hook says no"};
}

function precommit_crash(object)
{
    throw "wobbler";
}

function postcommit_ok(object)
{
    return ok;
}

function postcommit_crash(object)
{
    throw "postcommit_crash";
}