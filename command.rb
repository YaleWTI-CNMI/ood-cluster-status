require 'open3'

class Command
  def to_s(node_type, partition_type)
#   "/var/www/ood/apps/sys/ycrc_cluster_status/cluster_status.py "+node_type+" "+partition_type
    "/gpfs/radev/apps/services/ood/share/apps/cluster_status/cluster_status.py "+node_type+" "+partition_type
  end

  AppNodeStatus = Struct.new(:partition, :node_color, :nodename, :cpu_color, :cpu_tot, :cpu_alloc, :gpu_color, :gpu_type, :gpu_tot, :gpu_alloc, :mem_color, :mem_tot, :mem_alloc)

  # Parse a string output from the `cluster_status` command and return an array of
  # AppNodeStatus objects
  def parse(output)
    lines = output.strip.split("\n")
    lines.map do |line|
      AppNodeStatus.new(*(line.split(";", 13)))
    end
  end

  # Execute the command, and parse the output, returning an array of
  # AppNodeStatus and nil for the error string.
  #
  # returns [Array<Array<AppNodeStatus>, String] i.e.[NodeStatus, error]
  def exec
    gpu_public_node_status, error1 = [], nil

    stdout_str, stderr_str, status = Open3.capture3(to_s("GPU", "public"))
    if status.success?
      gpu_public_node_status = parse(stdout_str)
    else
      error1 = "Command '#{to_s}' exited with error: #{stderr_str}"
    end

    cpu_public_node_status, error2 = [], nil

    stdout_str, stderr_str, status = Open3.capture3(to_s("CPU", "public"))
    if status.success?
      cpu_public_node_status = parse(stdout_str)
    else
      error2 = "Command '#{to_s}' exited with error: #{stderr_str}"
    end

    gpu_private_node_status, error3 = [], nil

    stdout_str, stderr_str, status = Open3.capture3(to_s("GPU", "private"))
    if status.success?
      gpu_private_node_status = parse(stdout_str)
    else
      error3 = "Command '#{to_s}' exited with error: #{stderr_str}"
    end

    cpu_private_node_status, error4 = [], nil

    stdout_str, stderr_str, status = Open3.capture3(to_s("CPU", "private"))
    if status.success?
      cpu_private_node_status = parse(stdout_str)
    else
      error4 = "Command '#{to_s}' exited with error: #{stderr_str}"
    end
    [gpu_public_node_status, cpu_public_node_status, 
     gpu_private_node_status, cpu_private_node_status,
     error1, error2, error3, error4]
  end
end

@command = Command.new
@gpu_public_node_status, @cpu_public_node_status,
@gpu_private_node_status, @cpu_private_node_status,
@error1, @error2, @error3, @error4 = @command.exec

puts(@gpu_public_node_status)
puts(@cpu_public_node_status)
