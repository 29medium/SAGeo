# Script de execução do modelo

import sys
import json
import matplotlib.pyplot as plt
import traceback
import os
import time
from models.DBSM.model import Model as DBSM
from models.Spanner.model import Model as Spanner
from models.Wren.model import Model as Wren


# Função responsável por mostrar os resultados
def showResults(results, filename, times):
    # Criação da figura
    fig, ax = plt.subplots(constrained_layout=True, figsize=(15, 7))
    fig.suptitle("Modelo de simulação de bases de dados geo-replicadas", fontsize=18)

    max_time = round(max(times),2)
    min_time = round(min(times),2)
    total_time = round(sum(times), 2)

    for algorithm, (res, sim_time) in results.items():
        # Resultados recolhido
        rt = list(
            map(
                lambda x: round(x[0]["total"] / (x[1]["total"] * 1000), 3)
                if x[1]["total"] != 0
                else 0,
                res.values(),
            )
        )

        througput = list(
            map(
                lambda x: round((x[1]["total"] / sim_time) * 1000, 3),
                res.values(),
            )
        )

        if algorithm == "DBSM":
            color = "blue"
        elif algorithm == "Spanner":
            color = "orange"
        elif algorithm == "Wren":
            color = "green"
        elif algorithm == "Wren Without Cache":
            color = "red"

        # Gráfico débito -> tempo de resposta
        ax.plot(througput, rt, marker="o", label=algorithm, color=color)

    ax.set_xlabel("Débito (1000 TX/s)")
    ax.set_ylabel("Tempo de Resposta (ms)")
    ax.legend()
    ax.grid(linewidth=0.3)

    if os.path.exists(filename):
        os.remove(filename)

    plt.text(0, -0.1, f"Tempo máximo de simulação: {max_time} s", transform = ax.transAxes)
    plt.text(0, -0.15, f"Tempo mínimo de simulação: {min_time} s", transform = ax.transAxes)
    plt.text(0, -0.2, f"Tempo total de simulação: {total_time} s", transform = ax.transAxes)

    plt.savefig(
        fname=filename,
    )

    plt.close()


# Função responsável pela execução do modelo
def main():
    if len(sys.argv) < 3:
        print("Invalid number of arguments")
        return

    try:
        results = dict()
        times = list()

        for i in range(1, len(sys.argv) - 1):
            f = open(sys.argv[i])
            vars = json.load(f)

            res = dict()

            for value in vars["VARIATION_VALUES"]:
                vars[vars["VARIATION_VARIABLE"]] = value

                var_variable = vars["VARIATION_VARIABLE"]
                alg = vars["ALGORITHM"]

                print(f"Starting {alg} {var_variable}: {value}")

                start_time = time.time()

                if vars["ALGORITHM"] == "Wren":
                    model = Wren(vars)
                elif vars["ALGORITHM"] == "Spanner":
                    model = Spanner(vars)
                elif vars["ALGORITHM"] == "DBSM":
                    model = DBSM(vars)

                model.run()
                transaction_time, transactions = model.results()

                times.append(time.time() - start_time)

                res[value] = [
                    transaction_time,
                    transactions,
                ]

                if vars["ALGORITHM"] == "Wren" and vars["CLIENT_CACHE"] == False:
                    algorithm = "Wren Without Cache"
                else:
                    algorithm = vars["ALGORITHM"]

                results[algorithm] = (res, vars["MAX_SIM_TIME"])

        showResults(results, sys.argv[-1], times)

    except:
        traceback.print_exc()


if __name__ == "__main__":
    main()
