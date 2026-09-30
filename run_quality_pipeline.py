import argparse,subprocess,sys

def run(module,*args):
    cmd=[sys.executable,"-m",module,*args]
    print("\n>"," ".join(cmd));subprocess.run(cmd,check=True)

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--rows",type=int,default=250000);a=p.parse_args()
    run("src.generators.generate_orders","--rows",str(a.rows))
    run("src.quality.run_quality_checks")
    run("src.observability.build_observability_metrics")
    run("src.observability.export_prometheus")
    run("src.reports.print_quality_report")
    print("\nQuality and observability pipeline completed.")
