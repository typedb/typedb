class GraknCore < Formula
  desc "Grakn Core: The Knowledge Graph"
  homepage "https://grakn.ai"
  url "https://github.com/graknlabs/grakn/releases/download/{version}/grakn-core-all-mac-{version}.zip"
  sha256 "{sha256}"

  bottle :unneeded

  depends_on :java => "1.8"

  def install
    # unpack Grakn Core distribution
    libexec.install Dir["*"]
    # set up db directory
    dbpath = (var/name/"db/")
    dbpath.mkpath
    rm_rf libexec/"server/db"
    ln_s dbpath, libexec/"server/db"
    # set up logs directory
    logpath = (var/name/"log/")
    logpath.mkpath
    rm_rf libexec/"server/logs"
    ln_s logpath, libexec/"server/logs"
    # install grakn as system-wide command
    bin.install libexec/"grakn"
    bin.env_script_all_files(libexec, Language::Java.java_home_env("1.8"))
  end

  test do
    assert_match /RUNNING/i, shell_output("#{bin}/grakn server status")
  end
end
