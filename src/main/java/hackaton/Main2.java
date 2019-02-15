package hackaton;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class Main2 {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
	long startTime = System.currentTimeMillis();

	Set<String> etapas2017 = new HashSet(
		Arrays.asList("4", "5", "6", "7", "8", "9", "10", "14", "15", "16", "17", "18", "19", "20", "21"));
	Set<String> etapas2018 = new HashSet(Arrays.asList("4", "5", "6", "7", "8", "9", "10", "11", "14", "15", "16",
		"17", "18", "19", "20", "21", "41"));
	ExecutorService executor = Executors.newCachedThreadPool();
	Callable<Map<String, Censo>> callable2017 = new Callable() {
	    public Map<String, Censo> call() throws IOException {
		return readFile("MATRICULA_SUDESTE_2017.CSV");
	    }
	};
	Future<Map<String, Censo>> future2017 = executor.submit(callable2017);

	Callable<Map<String, Censo>> callable2018 = new Callable() {
	    public Map<String, Censo> call() throws IOException {
		return readFile("MATRICULA_SUDESTE_2018.CSV");
	    }
	};
	Future<Map<String, Censo>> future2018 = executor.submit(callable2018);

	System.out.println(System.currentTimeMillis() - startTime);
	Map<String, Censo> censos2017 = future2017.get();
	Map<String, Censo> censos2018 = future2018.get();
	System.out.println(System.currentTimeMillis() - startTime);
	executor.shutdown();

	Set<String> abandono = new HashSet<>(censos2017.keySet());
	abandono.removeAll(censos2018.keySet());
	for (Iterator<String> i = abandono.iterator(); i.hasNext();) {
	    Censo censo = censos2017.get(i.next());
	    if (!etapas2017.contains(censo.etapaEnsino)) {
		i.remove();
	    }
	}
	System.out.println(System.currentTimeMillis() - startTime);

	Set<String> regular = new HashSet<>(censos2018.keySet());
	regular.retainAll(censos2017.keySet());
	for (Iterator<String> i = regular.iterator(); i.hasNext();) {
	    Censo censo = censos2018.get(i.next());
	    if (!etapas2018.contains(censo.etapaEnsino)) {
		i.remove();
	    }
	}
	System.out.println(System.currentTimeMillis() - startTime);

	Set<String> novos = new HashSet<>(censos2018.keySet());
	novos.removeAll(censos2017.keySet());
	for (Iterator<String> i = novos.iterator(); i.hasNext();) {
	    Censo censo = censos2018.get(i.next());
	    if (!etapas2017.contains(censo.etapaEnsino)) {
		i.remove();
	    }
	}
	System.out.println(System.currentTimeMillis() - startTime);

	// for (String matricula:abandono) {
	// System.out.println(matricula);
	// }
	System.out.println("abandono=" + abandono.size());
	System.out.println("regular=" + regular.size());
	System.out.println("novos=" + novos.size());
	System.out.println(System.currentTimeMillis() - startTime);

	List<Censo> censosAbandonoRegular = new ArrayList<>();
	for (String matricula : abandono) {
	    Censo censo = censos2017.get(matricula);
	    censo.abandono = 1;
	    censosAbandonoRegular.add(censo);
	}
	for (String matricula : regular) {
	    Censo censo = censos2017.get(matricula);
	    censo.abandono = 0;
	    censosAbandonoRegular.add(censo);
	}

	List<Censo> censosRegularNovos = new ArrayList<>();
	for (String matricula : regular) {
	    Censo censo = censos2018.get(matricula);
	    censosRegularNovos.add(censo);
	}
	for (String matricula : novos) {
	    Censo censo = censos2018.get(matricula);
	    censosRegularNovos.add(censo);
	}
	System.out.println(System.currentTimeMillis() - startTime);
	
	List<Censo> listaReduzida = new ArrayList<>();
	for (int i = 0; i < 1000;++i) {
	    listaReduzida.add(censosRegularNovos.get(i));
	}
	censosRegularNovos = listaReduzida;
	
	long lastTime = System.currentTimeMillis();
	
	System.out.println("Classificacao");
	for (int i = 0; i < censosRegularNovos.size(); ++i) {
	    Censo censoI = censosRegularNovos.get(i);
	    for (Censo censoJ : censosAbandonoRegular) {
		int d = 0;
		if (!censoI.corRaca.equals(censoJ.corRaca)) {
		    ++d;
		}
		if (!censoI.etapaEnsino.equals(censoJ.etapaEnsino)) {
		    ++d;
		}
		if (!censoI.necessidadeEspecial.equals(censoJ.necessidadeEspecial)) {
		    ++d;
		}
		if (!censoI.zonaResidencial.equals(censoJ.zonaResidencial)) {
		    ++d;
		}
		censoJ.d = d;
	    }
	    Collections.sort(censosAbandonoRegular, new Comparator<Censo>() {
		@Override
		public int compare(Censo o1, Censo o2) {
		    return o1.d - o2.d;
		}
	    });

	    int risco = 0;
	    for (int j = 0; j < 100; ++j) {
		risco += censosAbandonoRegular.get(j).abandono;
	    }
	    censoI.risco = risco;
	    
	    if (System.currentTimeMillis() > lastTime + 1000) {
		lastTime +=1000;
		System.out.println(i*100/(double)censosRegularNovos.size()+"%");
	    }
	    
	}
	System.out.println(System.currentTimeMillis() - startTime);

	PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File("risco.csv"))));
	for (Censo censo : censosRegularNovos) {
	    out.print(censo.idMatricula);
	    out.print(",");
	    out.println(censo.risco);
	}
	out.flush();
	out.close();
	System.out.println(System.currentTimeMillis() - startTime);
    }

    static Map<String, Censo> readFile(String file) throws IOException {
	String[] header;
	{
	    DataInputStream in = new DataInputStream(new FileInputStream(new File(file)));
	    header = in.readLine().split("\\|");
	    in.close();
	}

	Map<String, Censo> censos = new HashMap<>();

	Reader in = new BufferedReader(new FileReader(file));
	CSVParser records = CSVFormat.newFormat('|').withHeader(header).parse(in);
	Iterator<CSVRecord> i = records.iterator();
	i.next();
	// int j = 0;
	for (; i.hasNext();) {// & j < 100000; ++j) {
	    Censo censo = new Censo();

	    CSVRecord record = i.next();
	    censo.idMatricula = record.get("ID_MATRICULA");
	    // censo.idMatricula = record.get(colunaCodigoAluno);
	    // System.out.println("\""+censo.idMatricula +"\"");

	    censo.corRaca = record.get("TP_COR_RACA");
	    censo.zonaResidencial = record.get("TP_ZONA_RESIDENCIAL");
	    censo.necessidadeEspecial = record.get("IN_NECESSIDADE_ESPECIAL");
	    censo.etapaEnsino = record.get("TP_ETAPA_ENSINO");

	    censos.put(censo.idMatricula, censo);

	    // censo.codigoEscola=record.get("CO_ENTIDADE");
	    // censo.codigoEscola=record.get("NU_DIA");
	    // censo.codigoEscola=record.get("NU_MES");
	    // censo.codigoEscola=record.get("NU_ANO");
	    // censo.codigoEscola=record.get("TP_SEXO");
	    // censo.codigoEscola=record.get("TP_NACIONALIDADE");
	    // censo.codigoEscola=record.get("CO_PAIS_ORIGEM");
	    // censo.codigoEscola=record.get("CO_UF_NASC");
	}
	return censos;
    }

}

class Censo {

    int abandono;
    int d;
    int risco;

    String idMatricula;
    String corRaca;
    String zonaResidencial;
    String necessidadeEspecial;
    String etapaEnsino;

}