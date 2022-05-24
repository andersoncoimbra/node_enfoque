
// Import net module.
var net = require('net');
// Firebase
const firebase 	= require("firebase-admin");
// Variaves de ambiente
require('dotenv').config();
//MYSQL
var mysql      	= require('mysql');
// Configuração do Firebase
var serviceAccount = require("./firebase.json");


firebase.initializeApp({
    credential: firebase.credential.cert(serviceAccount),
    databaseURL: process.env.FIREBASE_URL
  });

/// Configuração do banco de dados
  var connection = mysql.createConnection({
    host     : process.env.DB_HOST,
    user     : process.env.DB_USER,
    password : process.env.DB_PASS,
    database : process.env.DB_NAME
  });
  connection.connect();

// Variavel para armazenar os papeis
var papeis;
var  opcoes;
var fundos;

/// Configuração de WebSocket
  function getConn(connName){
    var option = {
        host:'socket.enfoque.com.br',
        port: 8090
    }
    // Create TCP client.
    var client = net.createConnection(option, function () {
        console.log('Connection name : ' + connName);
        console.log('Connection local address : ' + client.localAddress + ":" + client.localPort);
        console.log('Connection remote address : ' + client.remoteAddress + ":" + client.remotePort);
    });
    client.setTimeout(10000);
    client.setEncoding('utf8');
    // When receive server send back data.
    client.on('data', function (data) {
        const d = data.toString('utf-8')
        //console.log('Received : ' );
        //console.log('Received : ' + d);

        const quoteT = quoteTArray(d);
        const quoteA = d.match(/([A]:)[^\r]*/g) || [];
        const quoteN = d.match(/([N]:)[^\r]*/g) || [];
        const quoteV = d.match(/([V]:)[^\r]*/g) || [];
        const quoteO = d.match(/([O]:)[^\r]*/g) || [];
        //console.log('Received : ' + quoteO);

        //console.log(quoteT);
        if(quoteT){           
            quoteFirebase(quoteT.ticker, quoteT.quote, quoteT.tipo);            
        }

    });
    // When connection disconnected.
    client.on('end',function () {
        console.log('Client socket disconnect. ');
    });
    client.on('timeout', function () {
        console.log('Client connection timeout. ');
    });
    client.on('error', function (err) {
        console.error(JSON.stringify(err));
    });
    return client;
}
// Create a java client socket.
var clients = getConn('ENFOQUE COTAÇÕES');
clients.write('L:'+process.env.USER_ENFOQUE+":"+process.env.PASS_ENFOQUE+":NMB+NTM+TRD");

//Query para buscar  Opções
connection.query('select opcaos.cod_empresa, opcaos.cod,  usuario_carteira.ticker, \
opcaos.empresa, opcaos.empresa_acao, opcaos.tipo, opcaos.vencimento_normal, opcaos.vencimento  \
FROM usuario_carteira \
   INNER JOIN opcaos on usuario_carteira.ticker = opcaos.cod \
    WHERE usuario_carteira.tipo IN ("CALL", "PUT") \
    AND opcaos.vencimento_normal >= DATE(NOW()) \
    GROUP BY usuario_carteira.ticker \
    ORDER BY opcaos.vencimento_normal ASC', function (error, results, fields) {
    if (error) throw error;
    opcoes = results; 
    console.log('Total de Opçoes: '+opcoes.length);
    //opcoes.forEach(function(regMysql) {
        //console.log(regMysql);
        //clients.write(`S:${regMysql.cod.toLowerCase()}\r\n`);
    //})

  }); 

    //Query para buscar  Ações
  connection.query('select distinct papel from analises2 where analises2.ano = YEAR(now())-2', function (error, results, fields) {
    if (error) throw error;
    papeis = results; 
    console.log('Total de Açoes: '+papeis.length);
  });

  //Query para buscar Fundos
  connection.query('select max(batch) as ultimo from fundo_imobiliarios where deleted_at is null', function (error, resposta, fields) {
    if (error) throw error;
    batch = resposta[0].ultimo;
    console.log('Batch: '+batch);
        connection.query('select ticker from fundo_imobiliarios where deleted_at is null and batch=' + batch + ' ', function (error, results, fields) {
            fundos = results;
            console.log(fundos);
            //console.log('Total de Fundos: '+fundos.length);
        });
  });
  connection.end();

  setInterval(function(){

    if(opcoes){
        //clients.write(`S:IBOVR102:ULT:HOR\r\n`);
        opcoes.forEach(function(regMysql) {
            //console.log(regMysql);
            clients.write(`S:${regMysql.cod.toLowerCase()}\r\n`);
           
        })
    }
    if(papeis){
        papeis.forEach(function(regMysql) {
            //console.log(regMysql);
            clients.write(`S:${regMysql.papel.toLowerCase()}\r\n`);
        });
    }

    if(fundos){
        fundos.forEach(function(regMysql) {
            //console.log(regMysql);
            clients.write(`S:${regMysql.ticker.toLowerCase()}\r\n`);
        });
    }

    delay(25000);    
    console.log('Limpando Assinaturas');
    clients.write(`C:\r\n`);

}, 26000);

//Envia a os dados para o Firebase
function quoteFirebase(ticker, data, tipo) {
    if(tipo == 'opcao'){
        if( data.valor > 0){
            dados = {
                cod: data.cod,
                data_atualizacao: data.data_atualizacao,
                hora_atualizacao: data.hora_atualizacao,
                ticker: data.ticker,
                valor: data.valor, 
                vencimento: getvencimento(data) };
            firebase.database().ref('/opcoesValidas/'+ticker.toUpperCase()).set(dados);
            if(callOrPut(ticker) == 'CALL'){
                opc = opcaoBuilder(ticker, data);
                //console.log('CALL',opc);
                firebase.database().ref('/opcoes/compra/'+ticker.toUpperCase()).set(opc);
            }
            if(callOrPut(ticker) == 'PUT'){
                opc = opcaoBuilder(ticker, data);
                //console.log('PUT',opc);
                firebase.database().ref('/opcoes/venda/'+ticker.toUpperCase()).set(opc);
            }
        } 
    }
    if(tipo == 'acao'){
        if( data.cotacao > 0){
           console.log('Enviando Açoes para o Firebase: ',data, tipo, ticker);
            firebase.database().ref('/cotacoesnovo/'+ticker.toUpperCase()).set(data);
        }
    }

}
  

//Constroi um array data para o Firebase
function quoteTArray(d) {
    const data = d.toString('utf-8');
    values = data.match(/([T]:)[^\r]*/g);
    values = values?values.toString('utf-8').split(/[\t]+/):null;
    simbol =  values?values[0].replace('T:',''):null;
    array = [];
    tipo = '';
    quote = [];
    
    if(simbol){
        if(simbol.length == 7 || simbol.length == 8){
            tipo = 'opcao';
        }
        if(simbol.length == 5){
            tipo = 'acao';
        }
    }
    
    if(tipo == 'opcao'){
        array.cod = simbol;
        array.data_atualizacao = dataHoje();
        array.hora_atualizacao = hora_atualizacao();
        array.ticker = simbol;        
        array.valor = values[1];

        opcao = opcaoConsolidada(simbol);
        
        
        if(isCallOrPut(opcao.cod)){
         
            quoteS = quoteSArray(d);
            quoteA = quoteAArray(d);


            array.acao = opcao.cod_empresa;
            array.acao_busca = opcao.cod_empresa;
            array.cod_empresa = opcao.cod_empresa;
            array.compra = array.valor?array.valor:0.00;
            array.empresa = opcao.empresa;
            array.empresa_acao = opcao.empresa_acao;

            array.fechamento = values[6]?values[6]:0.00;
            preco = quoteS[0] == simbol?quoteS[3]:null;
            array.preco = preco?preco:'';

            ask = quoteA[0] == simbol? quoteA[10]:null;
            array.premio_put = ask?ask:0.00;

            bid = quoteA[0] == simbol? quoteA[9]:null;
            array.premio_call = bid?bid:0.00;

            array.vencimento = opcao.vencimento;
            array.venda = array.valor?array.valor:0.00;
        }   
        quote = array;
    }

    if(tipo == 'acao'){
        
        array.cotacao = values[1];
        array.data = dataHoje();
        array.papel = values[0].replace('T:','');
        array.variacao = values[3];
        quote =  array;
        //console.log(quote);
    }
    if(values){
    return {'quote': quote, 'tipo': tipo, 'ticker': simbol};
    }
    else{
        return false;
    }

}

function quoteSArray(s){
    const data = s.toString('utf-8');
    Squote = data.match(/([S]:)[^\r]*/g);
    Squote = Squote?Squote.toString('utf-8').split(/[\t]+/):null;
    if(Squote){
        Squote[0] = Squote[0].replace('S:','');
        return Squote;
    }else{
        return false;
    }

}

function quoteAArray(a){
    const data = a.toString('utf-8');
    Aquote = data.match(/([A]:)[^\r]*/g);
    Aquote = Aquote?Aquote.toString('utf-8').split(/[\t]+/):null;
    if(Aquote){
        Aquote[0] = Aquote[0].replace('A:','');
        return Aquote;
    }else{
        return false;
    }
}




function dataHoje()
{
    var dNow = new Date();
    var localdate = ("0" + dNow.getDate()).slice(-2) + '/'+("0" + (dNow.getMonth() + 1)).slice(-2) + '/' + dNow.getFullYear();
    return localdate;
}

function hora_atualizacao(){
    var dNow = new Date();
    var localdate = ("0" + dNow.getHours()).slice(-2) + ':' + ("0" + dNow.getMinutes()).slice(-2) + ':' + ("0" + dNow.getSeconds()).slice(-2);
    return localdate.toLocaleString('pt-BR', {timeZone: 'America/Sao_Paulo'});
}

  function vencimentoHoje()
{
	var dNow = new Date();
  	var localdate = dNow.getFullYear() + '' + ("0" + (dNow.getMonth() + 1)).slice(-2) + '' + ("0" + dNow.getDate()).slice(-2);

  	return localdate;
}

function segundoAtual()
{
    var dNow = new Date();
    var localdate = ("0" + dNow.getSeconds()).slice(-1);
    return localdate;
}

function getvencimento(array){

    data =  opcoes.find(function(opcao) {
        if (opcao.cod == array.cod) {
            //console.log('Encontrado: '+opcao);
          return opcao.vencimento_normal;
        }
      });
      if(data){
        return formatDates(data.vencimento_normal);
      }else{
          return '';
      }
}

//formata data de vencimento
function formatDates(data){

    if(data){
        localdate = data.getFullYear() + '-'+("0" + (data.getMonth() + 1)).slice(-2) + '-' + ("0" + data.getDate()).slice(-2);
        return localdate;
    }else
    {
        return '';
    }

}

function delay(ms) {
    return new Promise( resolve => setTimeout(resolve, ms) );
}

function isCallOrPut(ticker){
    letra  = ticker.substring(4,5);

    call = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L'];
    put = ['M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X'];

    numbercaracter = parseInt(ticker.length);    
    if(numbercaracter == 7 || numbercaracter == 8){
        if (call.includes(letra)) {
            return true;
        } //else {
        if (put.includes(letra)) {
            return true;
        }        
    }
    return false;




}
function callOrPut(ticker){
    letra  = ticker.substring(4,5);

    call = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L'];
    put = ['M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X'];


    if (call.includes(letra)) {
        return 'CALL';
    } //else {
    if (put.includes(letra)) {
        return 'PUT';
    }

}

function opcaoConsolidada(ticker){
    data =  opcoes.find(function(opcao) {
        if (opcao.cod == ticker) {
            return opcao;
        }
      });
      if(data){
        return data;
      }else{
        return false;
      }
}

function opcaoBuilder(ticker, data){
    opcao = {
        acao: data.acao,
        acao_busca: data.acao_busca,
        cod: data.cod,
        cod_empresa: data.cod_empresa,
        compra: data.compra,
        empresa: data.empresa,
        empresa_acao: data.empresa_acao,
        fechamento: data.fechamento,
        preco: data.preco,
        premio_call: data.premio_call,
        premio_put: data.premio_put,
        vencimento: data.vencimento,
        venda: data.valor        
    }

    opcaoEmpety = {
        acao: '',
        acao_busca: '',
        cod: '',
        cod_empresa: '',
        compra: '',
        empresa: '',
        empresa_acao: '',
        fechamento: '',
        preco: '',
        premio_call: '',
        premio_put: '',
        vencimento: '',
        venda: ''
    }


    if(isCallOrPut(ticker)){
        return opcao;
    }else{

    }

}
