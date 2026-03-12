import docker
import os

def execute_docker_sandbox(repo_workspace_dir: str, sandbox_config: dict) -> dict:
    """
    Step 13: The Docker Sandbox.
    Runs the AI's code in a secure, isolated container to prove it works.
    """
    client = docker.from_env()
    
    image = sandbox_config.get("image", "ubuntu:latest")
    setup_cmd = sandbox_config.get("setup", "")
    test_cmd = sandbox_config.get("test", "")
    fallback_cmd = sandbox_config.get("fallback", "echo 'No tests'")

    # Combine commands: setup first, then try tests. If tests don't exist, use fallback.
    # The '||' ensures if the test fails, we still catch the exit code.
    combined_command = f"sh -c '{setup_cmd} && ({test_cmd} || {fallback_cmd})'"
    
    try:
        # Spin up the container, mount the repo directory, and run the command
        container = client.containers.run(
            image=image,
            command=combined_command,
            volumes={os.path.abspath(repo_workspace_dir): {'bind': '/workspace', 'mode': 'rw'}},
            working_dir="/workspace",
            detach=True,
            mem_limit="512m", # Hard limit memory to prevent rogue code from crashing your server
            network_disabled=True # Disable internet access for extreme security (prevents crypto miners)
        )
        
        # Wait for the container to finish executing (timeout after 60 seconds)
        result = container.wait(timeout=60)
        logs = container.logs().decode("utf-8")
        
        # Cleanup the container immediately to free up server resources
        container.remove(force=True)
        
        exit_code = result.get("StatusCode", 1)
        
        if exit_code == 0:
            return {"status": "success", "console_log": "Tests passed successfully."}
        else:
            # Strip the log down to the last 1500 characters so we don't overwhelm the retry AI
            return {"status": "failed", "console_log": logs[-1500:]}
            
    except Exception as e:
        return {"status": "failed", "console_log": f"Sandbox execution error: {str(e)}"}