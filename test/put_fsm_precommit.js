function precommit_noop(object)
{
    return object;
}

function precommit_fail(object)
{
    return "fail";
}

function precommit_fail_reason(object)
{
    return {"fail":"the hook says no"};
}